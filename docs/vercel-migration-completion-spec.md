# Anata Agent Vercel Migration Completion Spec

Status: build-ready  
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
