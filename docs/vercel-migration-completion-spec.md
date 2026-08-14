# Anata Agent Vercel Migration Completion Spec

Status: execution in progress — not ready for production cutover

Target: a fully verified Vercel duplicate before any production-domain cutover

Production safety rule: Render and `agent.anatainc.com` remain unchanged until every gate below passes and the owner explicitly approves cutover.

## Plain-English outcome

Anata Agent will run on Vercel with the same pages, permissions, data, integrations, scheduled work, files, and audit trails that the Render application has today. The migration is complete only after the duplicate passes repeated automated, operational, and visual reviews; completes a controlled data rehearsal; and has a tested rollback path. The domain move is a separate, explicitly approved final action.

## Verified starting point

- A Vercel preview project and isolated Neon database exist.
- The FastAPI application deploys as one Vercel service in `pdx1`, beside Neon.
- Startup schema work is removed from the request path and Neon initialization is serialized.
- Website Ops has a PostgreSQL-backed durability boundary for its historical filesystem contract.
- Major authenticated desktop pages return successful responses and the shared shell has been visually reviewed.
- Render production has not been changed.
- Preview publishing, embedded schedulers, and external writes remain disabled for safety.
- Remaining gaps are callback/domain registration, durable scheduled execution, persistent artifact parity, integration write-path testing, cold-start proof, full page/state coverage, and cutover rehearsal.

## Current execution status (August 14, 2026)

The Vercel duplicate is real and functional, but it is not yet a production-equivalent replacement. The following evidence has already been verified:

- The staging project deploys from `codex/vercel-agent-duplicate` and uses an isolated Neon database in `pdx1`.
- The stable hostname `agent-staging.anatainc.com` resolves to Vercel with valid HTTPS. Authenticated fallback login and canonical staging navigation pass; provider callback registration remains open.
- Vercel schedules exist for Website Ops, Content, stale-lead scanning, Gmail ingestion, Sales operations, HR reminders, Building operations, and Outbound. They are authenticated and globally disabled with `VERCEL_CRON_WRITES_ENABLED=false`.
- Website Ops and Fulfillment retained-report caches no longer hydrate during application startup. The latest measured external cold readiness response was approximately 4.6 seconds, down from a startup path that previously exceeded two minutes.
- Fulfillment report files have a PostgreSQL-backed durable mirror with hash validation and lazy restore.
- Focused tests for cron authentication/leases, Building operations, Fulfillment storage, dashboards, Finance upload compatibility, intake compatibility, and canonical navigation pass.
- Render production and `agent.anatainc.com` remain unchanged.
- Release candidate code `aedbc4e` plus evidence-only commit `e38e96d` is deployed as Vercel deployment `dpl_mvLbqzjvW2n5fSFjHQHNAgwQFu5X`; Vercel reports it ready at the stable staging hostname.
- The hosted release gate passed against the exact application-code revision: 3,350 tests passed, one skipped, zero failed, plus 65 passing subtests. Finance renderer boundaries use and test the Denver operator business date so UTC midnight does not change the page or release result.
- Authenticated administrator login works on the immutable staging deployment. Ten major desktop sections render their canonical main navigation and expected page heading without browser console warnings or errors.
- Sales Deal Board, Fulfillment Pipeline, HR Dashboard, and Finance were visually reviewed at 1440 by 900. Their global header, section navigation, content alignment, spacing, cards, and empty states are cohesive and unclipped.
- The deployed desktop pass rechecked Workspace Home, Sales Deal Board, Fulfillment Prospects & Assets, Finance Today, and HR Dashboard. The shared full-width header, section navigation, content grid, cards, and empty states remain cohesive and unclipped.
- Shared Website Ops, Fulfillment, and HR report libraries were audited at 1280px. An HR section-menu clipping defect was fixed and verified on deployment `dpl_4Bux8yHmJqCq7X9ZKs1h7hB8eDRf`; all 14 HR links are visible and the console is clean.
- The callback registry, redacted environment-parity receipt, read-only database/artifact comparator, and cutover/rollback runbook now exist.
- Performance is closed for the current candidate: three fresh deployments returned five concurrent readiness responses in 0.6–1.1 seconds, all 200; ten authenticated representative pages remained under the ten-second cold-navigation ceiling and warm repeats were under two seconds.
- Personalized workspace home release `b032aec` is deployed as `dpl_DuYJ6n851cNVZkstd3MnqtVu5bFN`. The live desktop pass verified permission-filtered workspaces, assigned-work framing, ordered shortcut controls, recent-page persistence, revised Website Ops/Fulfillment/Executive labels, semantic main regions, and a clean browser console. Production remained unchanged.
- Migration hardening release `ff46421` is deployed as `dpl_BKU8CpgwVtjHW6HCsUityRESQ3kQ`. The comparator now proves sampled-record fingerprints, timestamp watermarks, and PostgreSQL sequence state in addition to schema, row counts, and artifact hashes. An authenticated read-only synthetic health schedule is registered; its unauthenticated live path rejects with 401 and all write schedules remain disabled.
- The production Render service is confirmed read-only as `Lead-scraper`, currently on commit `7d881c2`. Its source PostgreSQL database contains 171 tables and 223,016 rows at the August 13 inventory point.
- Render confirms the production service has no persistent disk. Retained state that must survive migration is database-backed, including `website_ops_files`, `content_artifacts`, and report tables; there is no separate Render disk archive to transfer.
- Staging now exposes an authenticated, read-only scheduler preflight contract. Its focused reliability/security checks and the hosted 3,350-test regression pass; the live unauthenticated path fails closed with 401.

The following items remain open and block a truthful claim of 100% completion:

| Gate | Remaining work | Evidence required to close | Owner help required |
| --- | --- | --- | --- |
| Stable staging identity | DNS and HTTPS are closed; register staging callback and webhook URLs with every provider | Successful provider login/logout and callback registry with pass/fail receipts | Provide access to provider consoles when an existing session is unavailable |
| Production data parity | Source access and baseline inventory are closed; export Render data, import/reconcile it in Neon, compare schema, counts, samples, sequences, and timestamps | Signed comparison report; repeatable full-plus-delta migration; successful restore rehearsal | Enter Vercel/Neon two-factor verification in the open Query console, or provide an approved migration window for the export/import |
| Artifact parity | Closed as a separate filesystem gate: Render has no persistent disk; retained artifacts are represented in PostgreSQL and move with the database | Database comparison must include `website_ops_files`, `content_artifacts`, and report tables; sample retained reports after import | None beyond the database migration access above |
| Durable execution | Inventory every `BackgroundTasks` path; move must-survive work to a durable queue/job; add any missing digest or synthetic-health schedule | Job ledger, overlap/retry tests, forced-failure receipt, no orphan Render schedule | Provider sandbox records only if controlled write testing needs them |
| Integration parity | Exercise OAuth, webhooks, reads, controlled writes, permission failures, and audit receipts for every major integration | Workflow matrix containing happy path, failure path, operator receipt, and source-system receipt | Login/approval in Google, QuickBooks, Plaid, HubSpot, ClickUp, Slack, Riverside, or other provider consoles as encountered |
| Regression suite | Closed: hosted full-suite pass completed against the exact deployed application-code revision | 3,350 passed, one skipped, zero failed, plus 65 passing subtests | None |
| Performance | Closed for the current candidate; repeat only after a material runtime/configuration change | Three fresh-deployment rounds and authenticated page timings recorded in `vercel-performance-receipt.md` | None |
| Product QA | Complete 1280px and shared-report coverage; test keyboard, restricted permissions, loading/error/stale states, and repeat fresh-eyes pass | Three complete QA passes against one immutable deployment; screenshot and log evidence | Restricted-role test account or approval to create one in staging |
| Rehearsal | Perform full snapshot plus delta rehearsal, validate jobs in shadow mode, prove rollback, time the runbook | Completed rehearsal record and rollback proof | Schedule a short rehearsal window and name the go/no-go and rollback owners |
| Production cutover | Final delta, domain/callback move, one-at-a-time job enablement, verification, and monitoring | Owner sign-off, successful cutover log, two clean business days | Explicit cutover approval; DNS/provider-console access during the window |

### Required external configuration

The engineering work should continue without waiting on these items, but these owner-controlled actions are mandatory before the corresponding gates can close:

1. Complete Vercel/Neon two-factor verification for the open read-only Query console so the target inventory and migration rehearsal can proceed.
2. Permit or perform staging callback registration in the external provider consoles, beginning with the confirmed Google redirect mismatch.
3. Supply a restricted-role staging account for permission QA.
4. Before cutover, name the go/no-go owner and rollback owner and explicitly approve the production move.

No secret value belongs in this document, source control, screenshots, or QA reports.

## Users

- Anata operators using Sales, Website Ops, Content, Finance, Building, Advertising, Executive, Fulfillment, and HR.
- Administrators managing access, integrations, jobs, and recovery.
- External systems calling OAuth callbacks, webhooks, or protected job endpoints.

## Non-negotiable constraints

- Preserve business logic, permissions, route contracts, Finance/Plaid behavior, and audit trails.
- Never run duplicate schedulers or duplicate external writes during rehearsal.
- Never expose secrets in source control, logs, screenshots, or QA artifacts.
- PostgreSQL is durable state. Vercel's local filesystem is only a temporary cache.
- Every external write is authenticated, idempotent where retry is possible, logged, and attributable.
- No production domain or DNS change occurs without explicit owner approval.
- Desktop at 1280px and 1440px is the current visual target. Mobile is deferred by owner direction, but the migration must not knowingly break existing responsive behavior.

## Completion phases

### Phase 1 — Establish a stable staging identity

Create a stable staging hostname, such as `agent-staging.anatainc.com`, for the Vercel project. Keep Vercel deployment protection or equivalent access control enabled. Register the stable hostname with every provider that requires an exact callback or webhook URL.

Work:

- Add the staging domain without changing `agent.anatainc.com`.
- Configure Google OAuth, QuickBooks, Plaid, Riverside/content connectors, Slack or other webhook consumers, and any provider-specific redirect allowlists.
- Replace production callback URLs in staging environment variables with staging URLs.
- Verify cookie security, SameSite behavior, redirect state validation, CSRF protection, and logout on the stable hostname.
- Document every provider, callback URL, responsible credential, and verification result without recording secret values.

Acceptance:

- Email/Google sign-in and authorized fallback login work on staging.
- Every OAuth flow returns to staging, not Render.
- Invalid or replayed OAuth state is rejected.
- Webhooks accept valid signatures and reject missing or invalid signatures.
- No staging workflow writes a production callback URL into generated content or notifications.

### Phase 2 — Complete durable data and artifact parity

Inventory every read and write location and assign it one durable owner: Neon, an approved object store, or an external source of truth. Remove any production dependency on Vercel's ephemeral filesystem.

Work:

- Compare Render database schema and row counts with Neon for every application table.
- Rehearse a fresh full export/import followed by a delta import while Render remains authoritative.
- Migrate Website Ops retained artifacts and verify file hashes, counts, timestamps, and report rendering.
- Move Fulfillment CS reports, uploaded files, generated decks, exports, and any other retained artifacts to durable storage or document why they are reproducible and intentionally not migrated.
- Verify transaction boundaries, uniqueness constraints, sequence values, timezone handling, and migration idempotency.
- Create backup, restore, and integrity-check commands with operator-readable output.

Acceptance:

- The durable-state inventory has no unexplained filesystem path.
- Schema comparison shows no missing tables, columns, indexes, or constraints.
- Required row counts and sampled records match the source after the rehearsal.
- Required artifact counts and hashes match.
- A clean staging database can be restored from backup and the application passes smoke tests afterward.
- Re-running the migration does not duplicate records or external effects.

### Phase 3 — Replace Render scheduling with durable Vercel-compatible jobs

Prepare every current Render cron or embedded scheduler as an independently callable, authenticated, idempotent job. Do not activate production schedules during staging.

Required job inventory:

- Website Ops hourly cycle
- Content hourly cycle
- Sales stale scan
- Gmail ingestion
- Sales operator cycle
- Building operations cycle
- Outbound morning cycle
- Synthetic journey/health validation
- Any additional Render cron discovered during final environment comparison

Work:

- Add secure GET adapters only where Vercel Cron requires GET; adapters must call the same service layer as the existing POST endpoint.
- Require a job secret or platform-verified cron identity.
- Add a run ledger containing job, requested time, start, finish, result, attempt, correlation ID, affected-record count, and error summary.
- Add idempotency keys and overlap protection so retries or concurrent invocations cannot duplicate Slack messages, CRM changes, emails, or publications.
- Replace in-request `BackgroundTasks` for work that must survive function termination with a durable queue or separately invoked job.
- Define timezones, schedules, maximum duration, retry policy, alert threshold, and manual replay procedure.
- Keep every external-write schedule disabled until cutover.

Acceptance:

- Each job can be invoked manually in dry-run/shadow mode and produces an auditable receipt.
- Replaying the same invocation produces no duplicate effect.
- A forced failure records the error, alerts the operator, and can be retried safely.
- Overlapping invocations cannot both acquire execution ownership.
- Every Render schedule has a mapped Vercel schedule and no orphan schedule remains.

### Phase 4 — Prove integrations and business workflows end to end

Test reads and controlled writes through the UI and APIs, using staging-safe records or provider sandboxes. Business logic must remain unchanged.

Coverage:

- Sales: ClickUp, HubSpot, Gmail, Instantly, Slack, lead queues, deal sync, audit receipts.
- Website Ops: plan execution, retained reports, site checks, history, error recovery.
- Content: Riverside ingestion, transformation, approval, shadow distribution, platform readiness, learning loop.
- Finance: Plaid link/callback/webhook, QuickBooks OAuth and reads, evidence labels, preview-confirm-receipt-undo, no unintended money movement.
- Building: intake, assignments, notifications, tours, operator jobs.
- Advertising: uploads, audits, recommendation review, exported corrections.
- Executive/shared reports: source freshness, links, access controls, empty/error states.
- Fulfillment: rate-sheet creation, uploads, generated decks, CS action queue, CS reports.
- HR: employee access, onboarding, policies, time/PTO, payroll readiness, compliance, reports, permission boundaries.

Acceptance:

- Each workflow has a recorded happy-path test and at least one rejected/error-path test.
- Every controlled write is visible in its source system and in Agent's audit history.
- Unauthorized roles receive a friendly denial and cannot reach protected data through direct URLs or APIs.
- Empty, loading, stale, partial-source, validation-error, upstream-error, and retry states are understandable and actionable.
- Finance continues to distinguish posted cash, confirmed receivables, expected income, required payments, and manual exceptions.

### Phase 5 — Performance and reliability gate

Measure the deployed application, not only local tests. Separate Vercel CLI overhead from application and browser timings.

Work:

- Capture cold and warm latency for readiness, login, and one representative page per major section.
- Record Vercel function startup milestones, request duration, database query time/count, response size, and upstream-call time.
- Confirm Vercel compute and Neon remain in the same region.
- Remove synchronous startup work that is not required to answer the request.
- Profile the Finance brief and other query-heavy pages; add bounded caching or query changes only when evidence supports it.
- Test database pool exhaustion, Neon suspend/resume, provider timeout, retry, and one failed deployment.
- Configure alerts for readiness failures, 5xx rate, job failures, and abnormal latency.

Acceptance thresholds:

- Readiness responds in under 1 second warm and under 5 seconds cold.
- Normal authenticated pages respond in under 2 seconds warm at p95.
- Heavy operational pages respond in under 4 seconds warm at p95 unless a documented upstream dependency is visibly in progress.
- Cold authenticated navigation completes in under 10 seconds and never returns 502/504.
- Ten sequential and five concurrent representative requests complete without database-pool or function errors.
- Three forced cold-start test rounds pass on separate deployments or after verified instance recycling.

### Phase 6 — Three-pass product QA

Run three independent passes against the exact release candidate. Any material fix resets the affected pass.

Pass A — Automated and structural:

- Full unit/integration suite, route inventory, schema checks, dependency/security scan, secret scan, link checks, and environment-key comparison.
- Verify every expected route returns the correct status and authentication behavior.

Pass B — Operator and visual:

- Desktop visual audit at 1280px and 1440px for every major page and shared report.
- Verify full-width global and section header bands, common 1320px alignment, typography, focus visibility, keyboard order, contrast, overflow, tables, dialogs, error summaries, empty/loading/error/stale states, and reduced motion.
- Exercise the application as administrator and at least one restricted role.

Pass C — Fresh-eyes regression:

- Repeat the critical workflows after at least one new deployment using a clean session.
- Compare screenshots and response metrics with the approved release candidate.
- Review Vercel logs, Neon activity, job ledger, and provider audit logs for silent errors or unexpected writes.

Acceptance:

- Zero severity-1 or severity-2 defects.
- No unexplained console error, 5xx response, failed job, permission leak, missing report, clipped header, horizontal overflow, or inaccessible critical action.
- Severity-3 defects are either fixed or explicitly accepted by the owner with a follow-up ticket and no migration risk.

### Phase 7 — Cutover rehearsal, production move, and rollback

The rehearsal must happen before the real domain change.

Rehearsal:

- Freeze staging writes.
- Restore a fresh production snapshot plus delta into the cutover target.
- Run migration checks, smoke tests, critical workflows, and read-only job checks.
- Measure total migration duration and write a minute-by-minute runbook.
- Restore the prior state to prove rollback.

Cutover prerequisites:

- All earlier gates signed off.
- DNS TTL reduced in advance.
- Final backup completed and restore verified.
- Render remains available but its schedulers are disabled immediately before Vercel schedulers are enabled.
- One named operator owns the go/no-go decision and one owns rollback.

Cutover order:

1. Enter a short maintenance/read-only window where required.
2. Stop Render schedulers and external writers.
3. Run the final delta migration and integrity check.
4. Promote the approved Vercel deployment without rebuilding it.
5. Point `agent.anatainc.com` to Vercel and update production callbacks.
6. Verify authentication, readiness, one page per section, and critical writes.
7. Enable Vercel jobs one at a time and confirm each receipt.
8. Monitor intensively for at least two business days while keeping Render rollback-ready.

Rollback triggers:

- Authentication failure affecting operators
- Data mismatch or missing durable artifacts
- Repeated 5xx/timeout failures
- Duplicate or missing scheduled writes
- Finance evidence or permission regression
- Any security or auditability failure

Rollback action:

- Disable Vercel jobs, restore the Render domain, restore callback URLs, re-enable Render schedules once, reconcile writes made during the Vercel window, and record the incident. Never allow both scheduler sets to run simultaneously.

## Required deliverables

- Environment-variable parity matrix with values redacted
- Stable staging-domain and callback registry
- Durable-state and artifact inventory
- Database migration and rollback scripts
- Job inventory, schedules, idempotency design, run ledger, and replay instructions
- Page/API/workflow QA matrix
- Performance report with cold/warm results
- Three-pass visual and accessibility QA report
- Cutover and rollback runbook
- Owner-facing QA links and a short checklist per page
- Final sign-off record naming the tested commit and immutable Vercel deployment

## Remaining execution order

Work proceeds in this order so that later QA is not invalidated by foundational changes:

1. **Close infrastructure inventory:** finish the environment, route, scheduler, background-task, durable-state, callback, and artifact inventories.
2. **Close durability:** complete Render-to-Neon migration tooling, artifact transfer, queue/job conversion, ledgering, idempotency, backup, and restore.
3. **Close staging identity:** make the stable hostname resolve and register/test every staging callback and webhook.
4. **Close workflow parity:** run the page/API/integration matrix with staging-safe data and resolve every permission, audit, and error-state defect.
5. **Create the release candidate:** run the clean full suite, security/secret/dependency checks, and performance tests; deploy one immutable candidate.
6. **Review the same candidate three times:** complete automated/structural, operator/visual, and fresh-session regression passes. A material code, schema, environment, callback, or job change invalidates the affected evidence and requires that pass to be repeated.
7. **Rehearse cutover and rollback:** time the final migration, prove rollback, and resolve all rehearsal findings before requesting approval.
8. **Cut over only after explicit approval:** move the production identity, enable jobs one at a time, and verify every critical workflow.
9. **Complete monitoring:** hold Render rollback-ready and review application logs, Neon activity, job receipts, integration audit logs, data counts, and operator reports during two full business days.

## Review and defect rules

- Severity 1: security exposure, permission leak, financial-data corruption, destructive or duplicate external write, widespread outage, or unrecoverable data loss. Stop immediately; the release cannot proceed.
- Severity 2: broken authentication, missing production-equivalent data/artifacts, recurring 5xx/timeout, failed critical workflow, failed scheduler, or incorrect audit evidence. Fix and repeat every affected gate.
- Severity 3: contained usability, visual, accessibility, or non-critical workflow defect. Fix before cutover unless the owner explicitly accepts it in writing and it presents no migration risk.
- Severity 4: cosmetic or documentation defect with no operational impact. Record it; it does not block cutover unless it violates the approved design system.
- A review is not complete when only the HTTP status is correct. The rendered page, browser console, network calls, permissions, data freshness, operator actions, audit receipt, and upstream side effect must all match the test case.
- Verbal confidence is not evidence. Each closed gate must link to a dated test report, screenshot set, log/query output, migration receipt, or provider audit receipt tied to the tested commit and deployment.

## Final owner sign-off record

The migration approval record must state:

- release commit and immutable Vercel deployment URL;
- database snapshot/export identifiers and final parity result;
- artifact inventory and integrity result;
- completed callback/provider registry;
- completed job inventory and last shadow receipts;
- results of QA Passes A, B, and C;
- rehearsal duration and rollback result;
- accepted non-blocking defects, if any;
- named go/no-go and rollback owners;
- explicit approval to change `agent.anatainc.com` and production callbacks;
- start and end of the two-business-day monitoring window.

## Explicit non-goals

- Redesigning business workflows during infrastructure migration
- Changing Finance or Plaid semantics
- Activating live Content distribution before its existing product gates pass
- Replacing external source systems
- Deleting Render immediately after cutover
- Treating mobile redesign as a prerequisite for this owner-approved desktop-first migration

## Recommended defaults for unresolved decisions

- Use `agent-staging.anatainc.com` for the stable preview identity.
- Keep Neon as the primary Vercel database and keep compute in `pdx1`.
- Use Vercel Cron for short orchestration and a durable queue for work that can outlive one request, retry, or fan out.
- Use private object storage for durable binary artifacts rather than storing large files in PostgreSQL.
- Keep staging in shadow/read-only mode by default; enable a write only for a named test and disable it immediately afterward.
- Retain Render rollback capability for at least seven days and two full business cycles after cutover.

## Definition of 100% complete

The migration is 100% complete only when all seven phases pass, all required deliverables exist, three QA passes succeed against the same immutable release candidate, the owner approves the production cutover, the controlled cutover succeeds, two business days of monitoring show no migration defect, and the rollback path remains verified. A successful Vercel deployment or a visually correct page alone is not completion.
