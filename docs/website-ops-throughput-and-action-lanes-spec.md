# Website Ops throughput and autonomous action lanes

Status: Build-ready product specification  
Owner: David Narayan  
Product surface: `https://agent.anatainc.com/admin/website-ops`  
Writable scope: production `anatainc.com` marketing pages only  
Schedule: 8:00 AM `America/Denver` plus weekly, monthly, and event-driven work  
Report recipient: `david@anatainc.com`  
Date: 2026-07-28

This document is a focused companion to
`docs/website-ops-autonomous-ranking-system-spec.md`. The autonomous-ranking
spec remains the complete product contract. This document defines the work
needed to increase safe production throughput and make that throughput
understandable.

## 1. Outcome

Website Ops must continuously turn verified SEO and AEO evidence into safe,
measurable production improvements.

It must be fast because independent low-risk work can proceed in parallel, not
because evidence or production safeguards are weakened.

The operator must be able to answer these questions in one scan:

1. What improved?
2. What did Agent consider?
3. What was rejected, disproved, or deferred, and why?
4. What can Agent fix automatically today?
5. Which action classes are not automated yet?
6. What is running next, on which URLs, and when?
7. Did prior changes improve indexing, visibility, engagement, or qualified
   leads?

## 2. Problem

The execution worker already processes every eligible queued action. The main
bottleneck is earlier in the system:

- candidate generation covers fewer action classes than the crawl can observe;
- only metadata, canonical, and validated new-article changes have complete
  production executors;
- FAQ insertion and page expansion are recognized but remain suggestion-only;
- internal links, broken links, schema, images, refreshes, and several other
  action classes cannot complete the full production loop;
- article generation selects one qualifying cluster and intentionally waits for
  repeated evidence and sources;
- a single global wait pattern makes deterministic repairs appear as slow as
  editorial decisions;
- unsupported, deferred, duplicate, disproved, and insufficient-evidence work
  has historically been under-explained.

The result is a safe system that can look inactive and cannot yet act on much of
the evidence it collects.

## 3. Current verified behavior

### Live

- Daily, weekly, monthly, and manual Website Ops runs exist.
- The production marketing sitemap defines 47 canonical route owners.
- Search Console and GA4 evidence are connected.
- The action runner executes all eligible actions in its queue.
- Metadata and validated article changes have repository publishing paths.
- Deployment and production-verification records exist.
- Report v2 is live and shows:
  - observed and validated candidate counts;
  - queued, ready, review-required, and executed counts;
  - reasons work did not run;
  - autonomous versus suggestion-only coverage;
  - article-pipeline state;
  - current and next operations.
- Changed reports email the same funnel context to David.

### Partial

- Canonical actions are supported, but URL-control actions remain governed by
  the high-risk policy.
- FAQ and existing-page expansion candidates can be generated but are not
  production executors.
- Crawl warnings are verified against rendered production, but many warning
  classes still need more specific deterministic checks.
- Outcome observations exist, but GA4 lead-event trust remains partial until a
  real service-page lead is reconciled with CRM.

### Missing

- complete autonomous lanes for internal links, broken links, existing-page
  content, FAQ/AEO blocks, structured data, images, sitemap cleanup, content
  refreshes, and safe consolidation;
- per-lane throughput budgets and risk gates;
- a durable record for every rejected or unsupported candidate;
- lane-level latency, success, rollback, and outcome reporting;
- automatic class suspension after repeated production failures.

## 4. Users

### David, owner

David needs outcomes and exceptions, not a crawler dump. He should see what
changed, what Agent will do next, why it is safe, and the few decisions only he
can make.

### Website Ops

Agent needs deterministic contracts for turning evidence into work, assigning
one intent owner, publishing within the marketing repository, validating
production, and learning from outcomes.

### Engineering operator

An engineer needs a complete audit trail, failure isolation, idempotent jobs,
rollback references, and enough evidence to reproduce every decision.

## 5. Scope

### Included

- candidate generation and classification;
- action-lane executors;
- risk-specific validation gates;
- bounded parallel execution;
- production publishing and verification;
- automatic rollback;
- reporting, email, and next-work planning;
- crawl, indexing, search, AEO, engagement, and lead outcome observations;
- marketing pages in the approved `anatainc.com` repository.

### Not included

- customer applications or authenticated product surfaces;
- DNS, OAuth, or app-subdomain changes;
- customer data or private knowledge indexes;
- manufactured links, paid ranking links, spam outreach, or deceptive content;
- autonomous deletion, migration, redirect, `noindex`, robots, or canonical
  changes outside the dedicated high-risk policy;
- invented search volume, keyword difficulty, rankings, facts, clients,
  results, or conversion attribution;
- replacing Google Search eligibility with a fictional separate AEO system.

## 6. Product principles

### 6.1 Capacity is a ceiling, not a quota

An action lane may execute up to its budget. It must execute zero changes when
zero candidates pass the evidence and risk gates.

### 6.2 Deterministic repairs should not wait for editorial evidence

A proven broken internal link does not need two weekly query cycles. A new
article does. Each action class receives the narrowest gate appropriate to its
risk.

### 6.3 Improve before creating

Existing pages with history, links, impressions, or an assigned intent owner
are improved before new pages are created unless the intent is materially
different.

### 6.4 Every candidate remains visible

Observed, duplicate, disproved, rejected, deferred, unsupported, queued,
executed, failed, rolled back, and measured candidates remain in the durable
ledger.

### 6.5 Parallelism cannot weaken isolation

Independent actions may run concurrently. Actions that touch the same file,
page, intent cluster, shared component, sitemap, or redirect table must be
serialized into one change set.

## 7. Improvement funnel

Every observation moves through this explicit state model:

```text
Observed
  -> Verifying
  -> Disproved | Noise | Duplicate | Validated
  -> Unsupported | Deferred | Evidence-qualified
  -> Queued
  -> Producing
  -> Validating
  -> Publishing
  -> Verifying production
  -> Completed | Failed
  -> Rolling back | Rolled back
  -> Measuring
  -> Learned
```

Every transition records:

- timestamp;
- responsible run and worker;
- evidence references;
- reason;
- confidence;
- applicable policy;
- previous and next state;
- earliest next eligibility time;
- human decision, when applicable.

## 8. Action lanes and initial budgets

Budgets are configurable and recorded with every run.

| Lane | Initial budget | Risk | Minimum evidence |
|---|---:|---|---|
| Broken internal links | 10 per run | Low | Crawl failure, rendered reproduction, repository source |
| Redirect-chain cleanup | 5 per run | Medium | Full chain, destination 200, intent preservation |
| Canonical and sitemap consistency | 5 per run | High | Desired-state record, repository, rendered production |
| Metadata corrections | 10 per run | Low | Intent owner plus observed query or deterministic defect |
| Contextual internal links | 10 per run | Low | Link-graph need, semantic relevance, unique destination owner |
| Existing-page content improvement | 3 per run | Medium | Observed gap, owner match, exact claim-supported copy |
| FAQ/AEO answer blocks | 3 per run | Medium | Repeated observed questions, visible answers, unique owner |
| Structured data | 5 per run | Low | Matching visible content and valid schema |
| Image semantics and delivery | 10 per run | Low | Rendered usage, asset evidence, accessibility or delivery defect |
| Content refresh | 3 per week | Medium | Material decay, staleness, or changed source evidence |
| New article | 1 per week initially | Medium | Validated gap, two sources, no cannibalization |
| Indexing reconciliation | All qualified URLs | Variable | Desired state plus status, canonical, robots, sitemap, links |

The new-article budget may increase to two per week only after four consecutive
articles pass factual, build, production, indexing, and quality reviews without
rollback or cannibalization.

## 9. Risk-specific gates

### 9.1 Deterministic technical repair

Required:

- production marketing scope;
- current rendered reproduction;
- exact repository location;
- deterministic correction;
- no intent or claim change;
- focused tests;
- production recrawl.

No weekly waiting period is required.

### 9.2 Metadata repair

Required:

- one canonical intent owner;
- current metadata;
- observed query evidence or deterministic metadata defect;
- proposed title or description within repository rules;
- no unsupported claim;
- rendered preview;
- production verification.

### 9.3 Internal link

Required:

- source and destination are canonical marketing URLs;
- destination owns the linked intent;
- source context makes the destination useful to a reader;
- anchor describes the destination;
- no duplicate link in the same context;
- rendered link is crawlable;
- source and destination return 200.

### 9.4 Visible content and FAQ

Required:

- observed search or approved customer-language evidence;
- one assigned owner;
- exact proposed copy;
- claim/source manifest;
- no confidential or invented facts;
- heading and accessibility validation;
- schema only when matching content is visible;
- production render and recrawl.

### 9.5 New article

Required:

- at least two distinct weekly validation cycles;
- at least two authoritative external sources;
- informational intent not owned by a suitable article or guide;
- source and claim manifests;
- differentiated value;
- internal links to and from relevant owners;
- canonical, article schema, sitemap inclusion, and production verification;
- scheduled indexing and outcome reviews.

### 9.6 High-risk URL action

Redirects, canonical ownership changes, consolidation, removals, robots, and
`noindex` require:

- dedicated policy approval;
- complete affected URL and backlink inventory;
- explicit desired state;
- traffic and intent preservation;
- bounded canary;
- rollback plan;
- post-release crawl and indexing reconciliation.

## 10. Executor contracts

Every lane implements the same interface:

1. `detect`: produce observations without recommending a change.
2. `verify`: reproduce the issue using current rendered and repository evidence.
3. `qualify`: apply scope, intent, evidence, claim, and risk policy.
4. `plan`: produce an exact bounded change and validation plan.
5. `execute`: create an isolated repository change set.
6. `validate`: run focused and repository-wide gates.
7. `publish`: commit and deploy through the approved marketing path.
8. `verify_production`: confirm deployment identity and expected rendered state.
9. `rollback`: revert only the responsible change and verify recovery.
10. `measure`: schedule suitable indexing and outcome observations.

An executor that cannot implement all required steps remains `suggestion_only`.

## 11. Parallel execution

### 11.1 Scheduling rules

- Each lane has its own queue and concurrency limit.
- A global coordinator leases work using durable idempotency keys.
- Work on different files and pages may run concurrently.
- Work sharing a file, page, intent owner, shared component, sitemap, or
  redirect table is grouped or serialized.
- Editorial actions never block deterministic technical repairs.
- Provider delays in one lane do not stop independent lanes.

### 11.2 Default concurrency

- Technical repair workers: 3
- Metadata and internal-link workers: 2
- Content workers: 1
- Deployment verifier: 1 per repository
- Outcome workers: 2

These defaults are lowered automatically after rate limits, repository
conflicts, deploy failures, or rollback events.

### 11.3 Stop conditions

Pause an action class when:

- two consecutive production verifications fail;
- rollback fails;
- unexpected files enter a change set;
- intent ownership changes during execution;
- a source becomes unavailable;
- an action causes an indexing, conversion, or page-experience regression
  beyond its configured guardrail.

## 12. Report and email contract

Report v2 is already live. The next implementation must complete its data,
drill-down, and outcome contracts.

### 12.1 Outcome summary

Show:

- changes shipped;
- affected URLs;
- verification and rollback state;
- indexing movement;
- non-branded visibility movement;
- engagement and trusted lead movement;
- data freshness and unavailable sources.

### 12.2 What happened today

For each change:

- before and after;
- exact production URL;
- evidence;
- action lane;
- commit and deployment;
- validation;
- production result;
- rollback reference;
- next outcome review.

### 12.3 Opportunity funnel

Show counts and drill-down URLs for:

- observed;
- verifying;
- validated;
- disproved;
- noise;
- duplicate;
- unsupported;
- deferred;
- queued;
- executing;
- completed;
- failed;
- rolled back;
- measuring.

Counts must use non-overlapping state definitions or explicitly label when one
candidate contributes multiple evidence observations.

### 12.4 Why work did not run

Every row includes:

- target URL or intent;
- exact reason;
- remaining gate;
- earliest eligible time;
- expected scheduler;
- whether David is needed;
- next automated operation.

### 12.5 Next operations

Show at least the next 12 durable work items, not a decorative shortlist.
Include lane, state, target, owner, expected start, dependency, and validation.

### 12.6 Coverage matrix

For every lane show:

- detection;
- verification;
- recommendation;
- execution;
- validation;
- production verification;
- rollback;
- measurement;
- current status and last successful canary.

### 12.7 System health

Show:

- scheduler and lease health;
- last successful daily, weekly, and monthly run;
- source freshness;
- queue depth and oldest-item age by lane;
- median evidence-to-production time;
- deployment and rollback failures;
- email delivery;
- paused action classes.

### 12.8 Email

Email only when meaningful state changes. It includes:

- what changed;
- how many candidates were observed, validated, ready, and executed;
- why other work did not run;
- what Agent will do next;
- what David must do, if anything;
- link to the full report.

## 13. Data model

### `Candidate`

- `candidate_id`
- `lane`
- `target_url`
- `intent_id`
- `observation_ids`
- `state`
- `state_reason`
- `confidence`
- `risk`
- `first_seen_at`
- `last_verified_at`
- `earliest_eligible_at`
- `required_gate`
- `executor_status`
- `duplicate_of`
- `work_id`

### `LaneDefinition`

- `lane_id`
- `enabled`
- `executor_status`
- `run_budget`
- `concurrency`
- `risk_policy`
- `required_evidence`
- `validation_suite`
- `pause_state`
- `pause_reason`
- `last_successful_canary`

### `WorkItem`

- `work_id`
- `candidate_id`
- exact proposed operation;
- target repository and files;
- dependencies and lock keys;
- validation plan;
- status and timestamps;
- retry and idempotency data;
- expected outcome window.

### `ChangeSet`

- `change_id`
- work items;
- exact diff;
- source and claim manifests;
- commit;
- deployment;
- production verification;
- rollback;
- notification;
- outcome schedule.

### `LaneOutcome`

- lane and action reference;
- comparable pre/post windows;
- source availability;
- technical, indexing, visibility, engagement, and trusted lead observations;
- confounders;
- association label;
- retain, revise, revert, pause, or promote decision.

## 14. API and service impacts

Required boundaries:

- candidate ledger service;
- lane registry and policy service;
- durable queue and lease service;
- per-lane detectors, verifiers, planners, and executors;
- repository change coordinator;
- deployment verifier and rollback service;
- outcome scheduler;
- report projection service;
- email notification service.

All writes are idempotent, auditable, retry-safe, and restricted to the
marketing repository.

## 15. UI states

Every lane and report supports:

- loading;
- ready;
- empty because no opportunity exists;
- empty because the executor is unsupported;
- waiting for evidence;
- waiting for scheduled window;
- queued;
- producing;
- validating;
- publishing;
- verifying production;
- measuring;
- blocked source;
- permission denied;
- stale;
- failed;
- rolling back;
- rolled back;
- paused action class.

An empty state always explains why it is empty and the next operation.

## 16. Accessibility and responsive behavior

- Keyboard-accessible controls and drill-down links
- Visible focus
- No color-only state
- One clear page heading
- Live regions for run and deployment state
- Tables with filters, result counts, and pagination
- Contained horizontal scrolling for necessary dense tables
- Phone layouts that preserve state, reason, target, and next operation
- Reduced-motion behavior
- No auto-refresh that steals focus

## 17. Rollout

### Phase 0: report truth

Status: substantially complete.

- Report v2 funnel
- Deferral explanations
- Coverage matrix summary
- Expanded next-work projection
- Updated email subject and body

Exit:

- David can distinguish no qualified work from an unsupported executor.

Remaining:

- non-overlapping candidate counts;
- candidate and coverage drill-downs;
- lane health and latency.

### Phase 1: candidate ledger and lane registry

- Persist every candidate and state transition.
- Define every lane and executor status.
- Add unsupported and deferred drill-downs.
- Record eligibility dates and next schedulers.

Exit:

- Every observed opportunity has one durable state and reason.

### Phase 2: deterministic technical lanes

- Broken internal links
- Metadata
- Contextual internal links
- Structured data
- Image semantics

Exit:

- Each lane completes one production canary with rollback proof.

### Phase 3: bounded parallel execution

- Durable lane queues
- Conflict locks
- Per-lane budgets
- Independent workers
- Class-level pause controls

Exit:

- At least two independent low-risk changes can complete in one run without
  collision or weakened validation.

### Phase 4: existing-page content

- Page expansion
- FAQ/AEO blocks
- Content refresh
- Claim and source manifests

Exit:

- Three existing-page changes complete the evidence-to-outcome loop without
  unsupported claims or cannibalization.

### Phase 5: advanced indexing and consolidation

- Redirect cleanup
- Canonical and sitemap reconciliation
- Content consolidation
- Retirement and safe redirects

Exit:

- High-risk canaries preserve intent, traffic paths, and rollback.

### Phase 6: outcome learning

- Lane-level indexing and outcome windows
- Promote, revise, pause, and revert decisions
- Throughput and value reporting

Exit:

- Prior observed outcomes influence lane priority without claiming causation.

## 18. Acceptance criteria

### Funnel and reporting

1. Every candidate has one current durable state.
2. Report funnel counts are non-overlapping or explicitly labeled as evidence
   observations.
3. Every deferred candidate has a reason, remaining gate, and next eligibility
   time.
4. Every unsupported candidate names the missing executor.
5. David can open the URLs behind every funnel count.
6. The report shows at least 12 next work items when they exist.
7. Email distinguishes zero safe work from zero executor coverage.

### Execution

8. Deterministic technical repairs do not wait for weekly editorial cycles.
9. Budgets cap work but never manufacture work.
10. Every eligible action in each lane can execute in the same run up to its
    budget.
11. Independent lanes may execute concurrently.
12. Conflicting files, pages, intents, and shared resources are serialized.
13. Unsupported executors cannot appear as autonomous.
14. Each production action records evidence, diff, commit, deployment,
    verification, rollback, and outcome schedule.
15. Failed verification rolls back only the responsible change.
16. Two consecutive verification failures pause the action class.

### Content and safety

17. Existing intent owners are improved before new pages are created.
18. FAQ content requires repeated observed questions and visible answers.
19. New articles require two weekly cycles, two authoritative sources, and no
    suitable existing owner.
20. No action invents facts, rankings, volume, difficulty, clients, results, or
    attribution.
21. No private or confidential data enters a public change.
22. High-risk URL actions remain blocked until their dedicated policy and
    canary pass.

### Operations

23. Redeploys and duplicate schedules cannot duplicate changes or email.
24. Queue latency and oldest-item age are visible by lane.
25. Provider failure pauses only dependent work.
26. Unchanged successful runs remain recorded without sending email.
27. Desktop and phone views retain target, state, reason, and next operation.
28. All controls are keyboard accessible and states are not color-only.

## 19. Validation plan

### Automated

- Candidate state-transition tests
- Non-overlapping report-count tests
- Lane budget and concurrency tests
- Conflict-lock tests
- Idempotency and retry tests
- Evidence and risk-policy tests per lane
- Repository diff-boundary tests
- Source and claim-manifest tests
- Build and rendered-page tests
- Deployment identity tests
- Production verification and rollback tests
- Email fingerprint tests
- Outcome-window tests

### Production canaries

Run in this order:

1. one broken internal-link correction;
2. one exact metadata correction;
3. one contextual internal link;
4. one visible-content-backed schema correction;
5. one image semantic correction;
6. two independent corrections in parallel;
7. one FAQ block;
8. one existing-page refresh;
9. one new article;
10. one high-risk indexing correction.

Every canary proves:

- evidence;
- exact diff;
- production deployment identity;
- rendered verification;
- rollback;
- report and email;
- scheduled outcome observation.

### Human QA

David confirms:

- the report explains why zero changes ran;
- every number opens a useful drill-down;
- next work includes a URL, timing, and remaining gate;
- coverage does not imply unsupported lanes are autonomous;
- before and after production changes are understandable;
- only real decisions appear under `Needs David`;
- emails explain outcomes rather than merely reporting counts.

## 20. Recommended defaults

- Start with the budgets in section 8.
- Allow three technical workers, two metadata/internal-link workers, one content
  worker, and one deployment verifier.
- Keep new articles at one per week until four clean production cycles.
- Keep outreach sending disabled.
- Keep high-risk URL actions canary-only.
- Improve before create.
- Pause an action class after two consecutive production-verification failures.
- Use the coded marketing repository until an approved CMS becomes
  authoritative.
- Email only on material state change.

## 21. Unresolved decisions

These do not block phases 1 and 2.

1. **Queue infrastructure**  
   Recommended default: use the existing durable database and worker pattern
   before adding a new provider.

2. **Maximum deployment batches per day**  
   Recommended default: one technical batch and one content batch, unless an
   urgent verified repair requires isolation.

3. **Outcome-window thresholds**  
   Recommended default: choose by action type and available evidence. Never
   invent statistical certainty.

4. **Automatic reversion for negative outcomes**  
   Recommended default: automatic rollback only for technical or verification
   failure. Outcome underperformance creates a revise/revert work item unless
   clear harm and a safe deterministic reversal exist.

5. **CMS ownership**  
   Recommended default: repository publishing remains authoritative until
   Sanity is configured, secured, and explicitly assigned content ownership.

## 22. First implementation sequence

1. Add the candidate ledger and lane registry.
2. Make report counts non-overlapping and add drill-downs.
3. Add eligibility dates, scheduler names, and executor blockers.
4. Build the broken-link executor and production canary.
5. Build the contextual internal-link executor and canary.
6. Build structured-data and image-semantic executors.
7. Add durable per-lane budgets, workers, and conflict locks.
8. Prove two independent corrections in one production run.
9. Build FAQ and existing-page expansion executors.
10. Build content-refresh publishing.
11. Add lane-level outcome learning and automatic class suspension.
12. Enable high-risk indexing canaries only after the lower-risk lanes are
    stable.
