# Finance Savings and AI CFO

Status: Smart CFO advisory foundation shipped

## Smart CFO Contract

Smart mode is not a ClickUp workflow. It is an explicit, cached LLM review of
the complete canonical Finance ledger. The system builds deterministic rollups
from every persisted transaction and obligation, then asks the model for at
most five concise recommendations across savings, collections, cash risk, and
data quality.

- The model may only cite record IDs supplied in the ledger packet.
- It cannot set cash, change the forecast, create a payable, close a task, or
  perform any source mutation.
- It runs only when the operator chooses **Run Smart review** and reuses the
  result while the ledger is unchanged.
- A missing `ANTHROPIC_API_KEY` disables only Smart CFO advice, not Finance.

ClickUp, QBO, and CSV remain inputs. ClickUp is optional and never a gate for
Smart CFO recommendations.

## Plain English Summary

### User outcome

Finance should not only explain cash and bills. It should also identify costs worth reviewing and show the operator where a cancellation, downgrade, duplicate correction, or renegotiation could improve cash without confusing that opportunity with money already saved.

The job is not to add a chatbot or another dashboard. The job is to add a controlled CFO review loop to the existing one-page Finance Control:

1. Find evidence-backed waste or cost increases.
2. Explain the evidence and the possible cash benefit.
3. Ask for the missing business facts.
4. Preview a safe follow-up action.
5. Track the decision and verify savings from later posted bank activity.

### What changes

`/admin/finances` remains the only Finance page. In Smart mode, a compact **Savings opportunities** section appears immediately after the Money queue and before the operating guide. It shows the three strongest opportunities and opens the existing right-side drawer for evidence review. It does not introduce another page, chart, KPI strip, or long queue.

The first release is deliberately narrow:

- Manual bank CSV remains the evidence source for posted expenses and realized savings.
- ClickUp may provide owner, notes, priority, and a follow-up destination.
- QBO may provide documented obligation context when connected.
- Deterministic rules calculate recurrence, confidence, potential value, and ranking.
- AI may summarize evidence, propose questions, and draft a follow-up message only after the facts and math are fixed.
- The system never cancels a service, sends a vendor message, moves money, or changes a forecast by itself.

### Why the foundation comes first

The current system can surface useful patterns, but several trust defects would make a savings estimate unsafe: destructive CSV replacement is still callable, blank bank transaction IDs can collapse distinct rows, recurrence can be inferred from only two observations, terminal ClickUp/QBO statuses can bypass allocation-based settlement, and the cash floor is fixed in code.

Those are Phase 0 blockers. Shipping an “AI CFO” before resolving them would create confident recommendations from data the operator already does not trust.

### What the user will notice

- A short ranked list such as “3 costs worth review, up to $1,120/month potential.”
- Every amount carries a horizon and state: one-time, monthly, annual, potential, approved, or realized.
- Selecting **Review** shows the exact transactions, calculation, freshness, confidence, limitations, and downside.
- Smart mode off keeps Finance focused on cash and bills; no savings recommendation is calculated into the visible forecast.
- Low-confidence or stale candidates ask for better data instead of recommending cancellation.
- Later phases add controlled follow-up and realized-savings verification without changing the one-page model.

## ASCII Mockup

### One-page placement and scan order

```text
+--------------------------------------------------------------------------------------+
| FINANCE CONTROL                         Updated 12m ago   Smart mode [ON]  [Update $] |
+--------------------------------------------------------------------------------------+
| Cash position -> Smart brief -> Cash trajectory -> Money queue                       |
+--------------------------------------------------------------------------------------+
| SAVINGS OPPORTUNITIES                                              Potential only    |
| 3 costs worth review                  Up to $225/mo recurring | $128 fees in 90d     |
|--------------------------------------------------------------------------------------|
| OPPORTUNITY       WHY SURFACED                 NEXT CHARGE   POTENTIAL   CONFIDENCE   |
| Design software   6 monthly charges; +22%      Jul 29        $45/mo      High [Review]|
| Storage vendor    5 charges; marked can-hold   Aug 02        $180/mo     Med  [Review]|
| Bank service fees 4 posted fees in 90 days     --            $128/90d    Med  [Review]|
|--------------------------------------------------------------------------------------|
| Estimates are not savings until later posted activity verifies the reduction.       |
+--------------------------------------------------------------------------------------+
| How to run Finance Control                                                           |
+--------------------------------------------------------------------------------------+
```

Only the top three eligible opportunities are shown. If more exist, **Review all _n_** expands the same section in place with a maximum of ten rows; it does not navigate to a new page. Money queue decisions remain above savings because cash and required obligations are the primary operator job.

### Evidence drawer

```text
+-------------------------------- SAVINGS REVIEW -------------------------------------+
| Design software                                               Potential: $45/month  |
| High data confidence | Bank CSV through Jul 13 | Not yet realized                  |
|                                                                                    |
| Why this surfaced                                                                  |
| Six monthly posted debits. Median rose from $204 to $249. Next expected Jul 29.    |
|                                                                                    |
| Evidence                                                                           |
| Jan 29  $204   Feb 28  $204   Mar 29  $204   Apr 29  $249   May 29  $249 ...       |
| Calculation: ($249 current - $204 baseline) x 12 = up to $540 annual gross.        |
| Unknown: contract term, cancellation fee, replacement cost, business owner.        |
|                                                                                    |
| Cash effect                                                                        |
| If reduced before Jul 29, 28-day stress minimum improves by up to $45.             |
| This scenario is not applied to the Finance forecast.                              |
|                                                                                    |
| Downside: service may support an active workflow; usage is not available.           |
|                                                                                    |
| [Close]                                             [Open source]                   |
+------------------------------------------------------------------------------------+
```

Phase 1B adds **Keep for now** and **Create review task**. Both open a confirmation preview. Neither cancels a vendor service or changes cash.

### Review-task preview

```text
+----------------------------- CREATE REVIEW TASK -----------------------------------+
| Title: Review Design software - potential $45/month                                |
| Owner: [Select owner]        Due: [Jul 22]                                          |
|                                                                                    |
| Includes: evidence summary, transaction dates, potential horizon, missing facts,   |
| source link, and the instruction “verify terms and usage before taking action.”     |
|                                                                                    |
| [Cancel]                                                    [Create in ClickUp]     |
+------------------------------------------------------------------------------------+
```

Creation is explicit, idempotent, and audited. Repeating the confirmation opens the existing task rather than creating a duplicate.

### Important states

**Smart mode off**

```text
| SAVINGS OPPORTUNITIES | Turn on Smart mode to review evidence-backed cost savings. |
```

**Loading**

```text
| SAVINGS OPPORTUNITIES | Finding savings opportunities... [three skeleton rows]     |
```

The section uses `aria-busy="true"` and announces completion through a live region.

**No eligible opportunities**

```text
| No evidence-backed savings opportunities need review.               [Update money] |
```

**Insufficient history**

```text
| More history is needed. Upload at least 90 days with three comparable charges.      |
|                                                                  [Update money]     |
```

**Stale source**

```text
| 2 possible opportunities | Estimates stale; cash impact unavailable.               |
|                                                               [Refresh sources]     |
```

Stale rows may be inspected as evidence, but follow-up actions are suppressed.

**Conflict or probable duplicate**

The candidate is removed from Savings opportunities and sent to Money queue **Needs action**. It cannot count toward potential savings until resolved.

**Section error**

```text
| Savings review is unavailable. Cash control remains current.  [Retry] [Update money]|
```

The failure is isolated to this section. It never hides or invalidates cash and bill controls.

## Technical Specification

### Scope

- Keep `/admin/finances` as the single canonical Finance route.
- Add one Smart-mode savings section after Money queue and before the operating guide.
- Add a deterministic savings evidence engine and a read-only evidence drawer.
- Complete the Phase 0 trust repairs before rendering savings estimates in production.
- In Phase 1B, persist review decisions and support preview-confirmed ClickUp follow-up creation.
- Reuse the current Finance page shell, table language, right drawer, data freshness labels, update flow, and accessibility patterns.

### Out of scope

- A general-purpose CFO chatbot or a separate Savings page.
- Autonomous cancellations, vendor communication, payments, forecast mutation, or accounting entries.
- Treating deferred payments, changed due dates, or cash timing as savings.
- Contract OCR, vendor web research, employee usage telemetry, procurement workflows, or new external integrations.
- Formal P&L, budgeting, general ledger, mobile-specific design, or multi-company accounting.
- Realized-savings reporting in Phase 1.

### Phase plan

#### Phase 0: Trust foundation

1. Remove `replace_range` from all CSV upload UI and reject it at the service boundary. Imports become append/merge only.
2. Route every source import through staging, preview, and `finance_source_records` before posting canonical events.
3. Harden blank-ID bank dedupe with a deterministic multiset identity:
   - use the external transaction ID when present;
   - otherwise compute a base fingerprint from account scope, posted date, signed amount, normalized description, and bank reference/check number;
   - append a stable occurrence ordinal within identical rows in the file;
   - reuploading the same multiset skips it, while an additional identical occurrence is preserved.
4. Require at least three comparable posted outflows for recurrence. Remove all two-observation production calls.
5. Add source-local status/open-balance fields to obligations. Make `settlement_allocations` the only local evidence of paid/open amount; ClickUp and QBO terminal labels remain source metadata until an allocation, cancellation evidence, or an explicit audited exception closes the obligation. QBO face amount and source-reported open balance must not both be reduced by the same local allocation.
6. Raise the auto-match threshold and fail ambiguous matches closed. Auto-allocation requires exactly one qualifying candidate and a defined lead over the runner-up; otherwise the item enters Money queue **Needs action**.
7. Add a persisted, operator-editable cash floor and make all summary, calendar, scenario, and recommendation code use it instead of a fixed `$10,000` constant.
8. Fail closed when settlement or source evidence cannot load: confidence becomes Low and only verification actions remain.
9. Reconcile pre/post migration totals and quarantine ambiguous source rows rather than silently merging them.

Phase 0 production gate: no Savings opportunity is displayed until these checks pass against a copy of production data.

Rollout control: `FINANCE_SAVINGS_MODE` defaults to `live` after the production-copy reconciliation. Use `off` to disable the check entirely or `shadow` for a future ruleset trial. Live mode remains conservative: candidates need posted CSV evidence, actions never alter the forecast, and realized savings require a later materially lower posted bank charge plus operator confirmation.

#### Phase 1A: Read-only Savings radar

- Derive recurring-cost, recurring-price-increase, and avoidable-fee opportunities from posted bank CSV outflows.
- Render the top three on Finance Control and expose complete evidence in the drawer.
- Show potential one-time, normalized monthly, and annual gross values separately.
- Calculate a scenario-only 28-day stress-path improvement, but do not apply it to Committed, Expected, Stress, cash on hand, safe-to-commit, or funding-gap metrics.
- Provide only **Close** and **Open source** actions.
- Run the engine in shadow mode for one production upload before exposing it in Smart mode.

#### Phase 1B: Controlled review workflow

- Add **Keep for now** with a default 90-day suppression.
- Add preview-confirmed **Create review task** in ClickUp.
- Record every decision, confirmation, source reference, evidence hash, actor, and timestamp.
- Keep stale, low-confidence, protected, or conflicted candidates read-only.

#### Phase 2: CFO qualification assistant

- Ask only the missing questions needed to qualify an opportunity: owner, necessity, contract end date, cancellation fee, minimum commitment, replacement cost, and target action.
- Allow AI to summarize the evidence packet, propose those questions, and draft a cancellation, downgrade, or negotiation message.
- Require operator edits and confirmation before any draft leaves Finance; sending remains out of scope until separately approved.
- Add states for qualified, approved, monitoring, dismissed, and reopened.

#### Phase 3: Closed-loop verification

- Watch future posted bank CSV activity for the normalized merchant and cadence.
- Mark a full recurring reduction realized only after two expected charge cycles pass without the debit.
- Mark a partial reduction realized only after two lower comparable charges post.
- Reopen the opportunity if the charge returns.
- Show expected versus realized savings inside the same section, not a new dashboard.

### Source authority and data flow

```text
Bank CSV -> stage -> preview -> source identity/dedupe -> posted transaction events
                                                        |
                                                        v
                                              recurrence/evidence engine
ClickUp -> obligations, owner, notes, priority ----------+
QBO ----> documented obligation context ----------------+
                                                        |
                                                        v
                                          ranked savings opportunities
                                                        |
                                  Finance section -> evidence drawer
                                                        |
                                  Phase 1B decision/task audit state
                                                        |
                                  future bank posts -> realized verifier
```

Authority rules:

- Posted bank CSV wins for actual cash movement and realized savings.
- `settlement_allocations` wins for obligation open balance.
- ClickUp supplies operational context, not proof that cash moved.
- QBO supplies documented obligation context; it does not override a conflicting posted-bank actual.
- Manual entries may create obligations or documented exceptions, but never fabricate posted bank movement.
- When sources disagree, the item enters Money queue **Needs action** and is excluded from savings totals.

Phase 0 schema additions:

```text
finance_settings
- scope_key (unique)
- cash_floor_cents
- active_actual_source
- updated_by
- created_at / updated_at

cash_events additions for obligations
- source_status
- source_open_amount_cents
- source_updated_at
```

The repository has no Alembic history. These additions use the existing additive SQLite/Postgres compatibility migration path in `models/database.py`, include an idempotent backfill, and must be verified against a production database copy before deployment.

### Opportunity contract

Add `sales_support_agent/services/cashflow/savings.py` as a pure, deterministic domain module. It should return an immutable view model with:

```text
SavingsOpportunity
- opportunity_key
- opportunity_type: recurring_cost | price_increase | avoidable_fee
- normalized_merchant
- display_name
- category
- evidence_transaction_ids[]
- evidence_dates[]
- evidence_amounts_cents[]
- baseline_amount_cents
- current_amount_cents
- next_expected_date
- one_time_potential_cents (nullable)
- monthly_potential_cents (nullable)
- annual_gross_potential_cents (nullable)
- verified_net_potential_cents (nullable)
- scenario_28d_floor_improvement_cents (nullable)
- data_confidence: high | medium | low
- decision_confidence: high | medium | low | unknown
- source_freshness
- evidence_hash
- reason_codes[]
- limitations[]
- downside
- protected
- conflicted
```

`opportunity_key` is a SHA-256 of account scope, normalized merchant, cadence, opportunity type, and direction. `evidence_hash` is a SHA-256 of the sorted evidence transaction identities, dates, signed cents, and latest source update time. A changed evidence hash expires previews and requires a new confirmation.

Phase 1B adds `finance_savings_reviews`:

```text
finance_savings_reviews
- id
- opportunity_key
- evidence_hash
- state: reviewing | kept | follow_up_created | dismissed
- suppression_until (nullable)
- reason (nullable)
- clickup_task_id (nullable)
- actor
- idempotency_key
- created_at / updated_at

finance_savings_review_events
- id
- review_id
- event_type
- prior_state / next_state
- evidence_hash
- payload_json
- actor
- idempotency_key
- created_at
```

The review table stores current operator state, and the events table is append-only audit history. Neither stores derived opportunity math; amounts are recomputed from current canonical evidence.

### Detection and math

Phase 1 supports recurring-cost, recurring-price-increase, and avoidable-fee candidates. A probable duplicate payment is treated as a risk first: it is routed to Money queue and may become one-time realized savings only after operator resolution proves it was avoidable.

Eligibility rules:

- Posted outflow transaction, not an obligation.
- Recurring candidates have at least three comparable charges from the same normalized merchant.
- Avoidable-fee candidates have an explicit fee description and either at least two posted fees or at least `$100` of fees in the last 90 days.
- No unresolved match, duplicate, transfer, refund, reversal, or source conflict.
- Not already represented in Money queue **Needs action**.
- Not a protected or must-pay category.
- A general recurring cost is eligible only when an associated obligation is explicitly marked `can_hold`; the system does not infer that a subscription is unused.
- Source history covers the candidate cadence and source freshness is within the Finance freshness policy.

Protected categories include payroll, tax, rent, debt service, insurance, and critical utilities. They may later receive renegotiation analysis, but they are never shown as cancellation candidates.

Supported initial cadences:

```text
weekly:     median interval 5-9 days
biweekly:   median interval 12-17 days
monthly:    median interval 25-35 days
quarterly:  median interval 80-100 days
```

Baseline is the median of the last three to six comparable posted debits. Normalized monthly potential is:

```text
weekly median * 52 / 12
biweekly median * 26 / 12
monthly median
quarterly median / 3
```

Annual gross potential is normalized monthly potential times 12. Price-increase potential is the positive difference between the current stable amount and the prior stable baseline, normalized to the cadence. Fee leakage is shown as the observed 90-day total; it is not annualized until at least 90 days of current source coverage exist. No savings amount is inferred from a changed payment date.

Net potential is:

```text
gross avoided charges
- known cancellation fees
- known committed minimums
- known replacement costs
- known transition costs
```

If any required deduction is unknown, `verified_net_potential_cents` is null and the UI says **Net savings unverified**. Unknown costs are never treated as zero.

### Confidence and ranking

Recurring data confidence:

- **High**: at least five comparable charges, amount coefficient of variation at or below 10%, cadence consistency at or above 80%, identifiable merchant, and current source coverage.
- **Medium**: at least three comparable charges, amount coefficient of variation at or below 25%, cadence consistency at or above 60%, and identifiable merchant.
- **Low**: fewer than three comparable charges, ambiguous merchant, stale source, unresolved transfer/refund, or source conflict. Low-confidence items are discovery evidence only and do not enter headline totals.

Fee confidence is **High** only with at least four explicit posted fee events, at least 90 days of current coverage, and no source conflict. It is **Medium** with two or three explicit fee events or at least `$100` observed in 90 days. Other fee candidates are Low.

Decision confidence is separate. It remains unknown until necessity, owner, terms, fees, and replacement cost are known. Overall action confidence is the lower of data confidence and decision confidence.

Ranking is deterministic and lexicographic:

1. High before medium data confidence.
2. Largest scenario funding-gap offset.
3. Earliest next expected charge.
4. Largest normalized monthly potential; for fee opportunities, largest observed 90-day amount.
5. Stable `opportunity_key` tie-breaker.

Headline totals include high- and medium-confidence eligible opportunities only. Mixed horizons are never summed into one number.

### AI boundary

Deterministic code owns source authority, dedupe, merchant identity after operator approval, recurrence, math, confidence thresholds, ranking, state transitions, expiry, idempotency, and audit records.

AI may:

- suggest a merchant alias for operator approval;
- summarize the fixed evidence packet in plain English;
- identify which business facts are missing;
- draft review questions or a cancellation/downgrade/negotiation message;
- rewrite a deterministic recommendation for clarity.

AI may not:

- invent contract terms, product usage, business criticality, fees, or replacement costs;
- calculate or alter savings values;
- change confidence, ranking, source authority, or review state;
- cancel a service, contact a vendor, move money, or write directly to ClickUp/QBO;
- cause a forecast mutation.

All AI output is schema-validated, labeled as a draft, tied to the evidence hash, and regenerated when evidence changes.

### State changes and actions

```text
detected (derived, not persisted)
  -> reviewing
  -> kept until date
  -> follow_up_created
  -> dismissed

Phase 2:
reviewing -> qualified -> approved -> monitoring
reviewing -> dismissed

Phase 3:
monitoring -> realized
monitoring -> reopened
```

- **Keep for now** suppresses the opportunity for 90 days by default and records a reason.
- **Create review task** uses preview/confirm, an idempotency key, and the current evidence hash.
- Expired evidence invalidates an unconfirmed preview.
- No Phase 1 action modifies a `cash_event`, `settlement_allocation`, forecast path, or bank transaction.

### Routes and renderer changes

- Continue rendering the primary surface from `GET /admin/finances`.
- Add a read-only evidence endpoint: `GET /admin/finances/savings/{opportunity_key}`.
- Phase 1B adds:
  - `POST /admin/finances/savings/{opportunity_key}/keep/preview`
  - `POST /admin/finances/savings/{opportunity_key}/keep/confirm`
  - `POST /admin/finances/savings/{opportunity_key}/follow-up/preview`
  - `POST /admin/finances/savings/{opportunity_key}/follow-up/confirm`
- Require the existing Finance admin authorization on every route.
- Use CSRF protection, evidence-hash validation, and idempotency keys on every write.
- Update `overview.py` so each drawer invocation receives its own evidence, calculation, confidence, and downside. Do not reuse the current top recommendation payload for queue or savings rows.
- Add Escape handling, focus trapping/restoration, background inerting, and live-region success announcements to the shared drawer.

Build map:

```text
sales_support_agent/models/entities.py
  finance_settings, source-local obligation fields, finance_savings_reviews

sales_support_agent/models/database.py
  additive SQLite/Postgres compatibility migration and backfill

sales_support_agent/services/cashflow/identity.py
  canonical source identity and blank-ID multiset fingerprinting

sales_support_agent/services/cashflow/imports.py
  atomic stage/classify/commit path

sales_support_agent/services/cashflow/upload.py
  parser adapter only; no direct destructive canonical writes

sales_support_agent/services/cashflow/control.py
  configured floor and fail-closed settlement/source behavior

sales_support_agent/services/cashflow/savings.py
  pure opportunity detection, evidence, confidence, math, ranking

sales_support_agent/services/cashflow/overview.py
  section, states, drawer payload, accessibility behavior

sales_support_agent/api/cashflow_router.py
  evidence and Phase 1B preview/confirm routes
```

### Validation and acceptance criteria

Phase 0:

- A `replace_range` request is rejected and cannot delete production rows.
- Reuploading the same blank-ID CSV inserts zero rows; adding one identical legitimate occurrence inserts exactly one row.
- ClickUp/QBO terminal status cannot reduce open balance without an allocation or audited cancellation exception.
- Every production recurrence call requires at least three observations.
- Editing the cash floor updates metrics, scenario analysis, and recommendations consistently.
- Pre/post migration transaction totals, obligation totals, and allocation totals reconcile; ambiguous rows are quarantined.

Phase 1:

- Given the same canonical events and `as_of` date, the engine returns byte-for-byte stable ordering and math.
- Protected, conflicted, stale, low-confidence, transfer, refund, and unresolved duplicate records are excluded from headline totals.
- Potential savings never alter cash on hand, Money queue balances, or any forecast path.
- Every displayed figure can be reproduced from the transactions shown in its drawer.
- Smart mode off shows only the compact opt-in placeholder.
- Empty, insufficient-history, loading, stale, conflict, and isolated-error states render as specified.
- The section and drawer pass keyboard-only navigation, focus restoration, screen-reader naming, and contrast checks.
- Desktop QA passes at 1280, 1440, and 1920 pixel widths without horizontal page overflow.
- Existing Finance control, import, queue, ClickUp, QBO, allocation, and redirect tests remain green.

Required new tests:

```text
tests/test_finance_savings_engine.py
tests/test_finance_savings_confidence.py
tests/test_finance_savings_routes.py
tests/test_finance_savings_renderer.py
tests/test_finance_savings_actions.py       # Phase 1B
tests/test_finance_source_identity.py       # Phase 0 blank-ID/multiset cases
tests/test_finance_settlement_authority.py  # Phase 0 source disagreement cases
```

### Risks and dependencies

- Bank descriptions may not uniquely identify a vendor. Ambiguous aliases remain low confidence until approved.
- Three months of CSV history cannot establish annual or irregular contracts; the system must say this rather than extrapolate.
- Missing usage and contract data means “cancel” is never the default action.
- Absence of one charge is not proof of cancellation; realized savings requires two expected cycles.
- ClickUp can store the follow-up but cannot prove money moved or a service stopped.
- QBO disconnection reduces obligation context but must not disable CSV-based read-only detection.
- Existing drawer payload reuse can show mismatched evidence and must be repaired before adding Savings review.

### Cross-surface consistency checks

- Money queue owns payment conflicts, probable duplicates, and already-paid review. Savings owns only resolved, avoidable cost opportunities.
- The Smart brief may reference the top eligible savings opportunity, but it must use the same engine output and evidence hash.
- The operating guide adds one weekly step: review savings opportunities after cash, incoming, and bills are current.
- Update money remains the only import entry point.
- Legacy Finance routes continue redirecting to `/admin/finances`; no hidden recurring or reconciliation page may calculate a second savings truth.
- Potential, approved, and realized labels use the same definitions in the section, drawer, ClickUp task, and future summaries.

### Exact implementation sequence

1. Ship Phase 0 trust foundation and production reconciliation tests.
2. Run the deterministic savings engine in shadow mode against one fresh production CSV upload and review false positives.
3. Ship Phase 1A read-only section and evidence drawer behind Smart mode.
4. After one week of operator review, ship Phase 1B suppression and preview-confirmed ClickUp follow-up.
5. Use reviewed evidence and decisions to specify Phase 2 qualification assistance; do not add generative AI before that data exists.
