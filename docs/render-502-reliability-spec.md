# Agent production reliability and 502 prevention

Status: implemented and production-verified  
Prepared: July 27, 2026  
Scope: `sales-support-agent` web service and its Render deployment lifecycle

Production verification completed July 27, 2026:

- release `72ab990` deployed successfully to the custom-domain service
  `sales-support-agent` on two Starter instances;
- Render uses the database-aware `/health/ready` gate;
- Website Ops durable storage contains 15 files / 323,662 bytes in PostgreSQL;
- embedded Website Ops and outbound schedulers are disabled;
- Render Cron owns both schedules with database-backed run leases;
- production promotion is manual because the available GitHub credential could
  not create a required-check workflow;
- 50 consecutive readiness samples remained `200` across the representative
  two-instance readiness-gated release, with no 502.

## Plain-English summary

Agent is not simply “running out of server.”

The web process currently has three jobs during a release:

1. prepare and repair the database;
2. start scheduled automation;
3. become the website that Render can send people to.

Those responsibilities are coupled together. At the same time, the health
check Render uses to decide whether Agent is safe does significant database
work and returns `200 OK` even when that database validation fails. Frequent
pushes repeatedly trigger this fragile startup and replacement sequence.

The recommended fix is to make deployment boring:

- prepare the database before the new website process starts;
- give Render a small, truthful readiness check;
- run scheduled automation outside the website process;
- deploy only tested commits, one release at a time;
- add a second web instance only after duplicate-job safety is proven.

Upgrading from Starter to Standard can provide performance headroom, but it is
not the first or primary fix for the observed deploy-time 502 window.

## Desired outcome

Normal Agent releases must not interrupt signed-in users, and a new version
must not receive traffic until it can safely serve authenticated pages and use
the production database.

No reliability change may duplicate emails, CRM writes, Website Ops actions,
outbound scans, Finance actions, or scheduled jobs.

## Discovery findings

### Verified facts

1. The repository Blueprint had accumulated a Website Ops disk declaration,
   while the live Render service had no disk attached. The declaration was
   removed, the live drift was reconciled, and Website Ops storage was migrated
   to PostgreSQL before multi-instance scaling.
2. During the July 26 release, production was observed in this sequence:
   healthy `200` responses, a short Cloudflare `502` window, then healthy
   `200` responses again.
3. Render normally keeps the old web instance serving until the replacement is
   healthy when there is no persistent disk.
4. Agent's `/health` endpoint currently performs approximately 13 SQL queries
   per request, including schema inspection and multiple row counts.
5. The endpoint catches database/schema validation exceptions and still
   returns `200 OK` with a validation-error detail. It is therefore not a
   truthful readiness gate.
6. Production health requests currently report roughly 31–51 ms of application
   time and 14–22 ms of database time, before public-network latency.
7. Before FastAPI finishes constructing the application, Agent runs database
   compatibility migrations, schema checks, legacy repairs, Building inquiry
   backfills, and super-admin seeding.
8. The current production branch starts embedded Website Ops and outbound
   scheduler threads inside the web process.
9. The Website Ops scheduler can begin due work immediately after its thread
   starts. The outbound scheduler also checks for due work immediately.
10. These embedded scheduler threads do not have application-lifecycle
    shutdown handlers in the current entrypoint.
11. The repository also has Render Cron services, establishing an existing
    pattern for scheduled work outside the web process.
12. `main` received many separate production commits on July 27, including
    commits only minutes apart. With `autoDeploy: true`, each eligible push can
    start another deployment.
13. Agent's current production commit is exposed by `/health`, which provides
    a reliable way to prove which version is actually serving.

### High-confidence diagnosis

The fragile point is the transition between old and new web processes:

- startup performs database and repair work before the server is available;
- embedded automation competes for resources and may begin work during startup;
- the health check is too heavy and cannot reject a broken database state;
- frequent automatic deploys repeatedly replace the only web instance.

This explains why a larger instance might reduce the probability of failure
without correcting the lifecycle design.

### Unknown until Render telemetry is connected

- Whether the observed 502 was accompanied by an out-of-memory event, process
  crash, failed health check, or Render networking event.
- Whether the live service configuration has drifted from `render.yaml`.
- CPU and memory utilization during startup and during embedded scheduled runs.
- The precise startup duration from process launch to first safe authenticated
  response.

These unknowns affect capacity selection, but they do not change the need for
truthful readiness and separation of web serving from scheduled work.

## Scope

### Included

- liveness and readiness endpoints;
- web-process startup and graceful shutdown;
- database migration/backfill execution;
- Website Ops and outbound scheduler ownership;
- Render deployment controls;
- deploy observability and alerts;
- safe prerequisites for multiple web instances;
- production validation and rollback.

### Not included

- changing Sales, HR, Finance, Fulfillment, or Website Ops business rules;
- changing Finance/Plaid behavior;
- changing permissions or authentication;
- redesigning pages;
- replacing Render;
- increasing capacity without measurement;
- changing the database provider.

## Target operating model

### Before a release

1. Automated tests and deployment checks pass for the selected commit.
2. A one-time pre-deploy command runs additive database migrations and
   idempotent repairs.
3. If pre-deploy work fails, Render keeps the existing version live.

### While the new version starts

1. `/health/live` confirms only that the Python web process is responsive.
2. `/health/ready` returns `503` until initialization is complete and a small
   database connectivity check succeeds.
3. Render sends no user traffic to the replacement until `/health/ready`
   returns `200`.
4. No scheduled scan or external write starts inside the new web process.

### During normal operation

1. Render Cron or a dedicated worker owns recurring automation.
2. Every scheduled run uses a database-backed lease/idempotency key so only one
   owner can execute it.
3. The web service serves requests and submits explicit background work; it
   does not own an always-running scheduler loop.

### During shutdown

1. The instance stops accepting new background work.
2. Request work receives the configured grace period.
3. Database connections and executors close cleanly.
4. Work intended to survive deploys is persisted, not left only in memory.

## Implementation phases

## Phase 0 — Capture the missing evidence

### Build

- Add structured startup milestones:
  `process_started`, `database_connected`, `schema_ready`, `app_ready`.
- Log the deployed commit and elapsed milliseconds for each milestone.
- Add structured shutdown milestones and executor/thread state.
- Record the incoming `CF-Ray` value in request/error logs without logging
  cookies, tokens, form bodies, or personal data.
- Configure Render deploy-failed, server-failed, and server-restarted
  notifications.
- Compare the live Render service settings with `render.yaml`, especially
  instance type, health-check path, disk attachment, branch, and auto-deploy.

### Acceptance criteria

- A deploy timeline can identify the exact commit, startup duration, readiness
  time, shutdown time, and any Render restart.
- A production 5xx can be matched to a request ID and deployment event.
- Logs contain no secrets or user data.

## Phase 1 — Replace the misleading health check

### Build

- Add `GET /health/live`.
  - No database query.
  - No external API call.
  - Returns `200` when the web process can respond.
- Add `GET /health/ready`.
  - Returns `503` until application initialization is complete.
  - Executes one bounded `SELECT 1` against the production database.
  - Returns `503` on timeout or database failure.
  - Includes only non-sensitive commit and readiness metadata.
- Move the current detailed diagnostic payload to an authenticated diagnostic
  route or a non-Render operational endpoint.
- Change `render.yaml` to use `/health/ready`.
- Keep `/health` temporarily as a backward-compatible alias, then deprecate it
  after monitors migrate.

### Acceptance criteria

- Render's readiness request performs at most one SQL query.
- A database outage causes `/health/ready` to return `503`.
- A schema diagnostic failure cannot be reported as healthy.
- Liveness responds without depending on PostgreSQL.
- Health endpoints finish within 250 ms at p95 inside the application.

## Phase 2 — Move database work out of web startup

### Build

- Create one explicit pre-deploy command for additive migrations and approved
  idempotent repairs.
- Use a PostgreSQL advisory lock so overlapping deploys cannot run the same
  migration concurrently.
- Move schema creation, compatibility migrations, legacy repairs, Building
  backfills, and required seeding out of module import/application construction.
- Keep application startup limited to configuration validation, connection-pool
  creation, route registration, and readiness state.
- Separate recurring data repair from schema migration when it does not need to
  block every release.
- Fail the deployment before promotion if required migration work fails.

### Acceptance criteria

- Importing `sales_support_agent.main` performs no DDL or bulk repair.
- A failed pre-deploy migration leaves the existing production version serving.
- Two attempted migrations cannot run concurrently.
- Re-running the pre-deploy command is safe.
- The replacement web process becomes ready in under 30 seconds.

## Phase 3 — Remove scheduled automation from the web process

### Build

- Make `WEBSITE_OPS_EMBEDDED_SCHEDULER` and
  `OUTBOUND_EMBEDDED_SCHEDULER` default to disabled.
- Give Render Cron jobs or a dedicated worker ownership of Website Ops and
  outbound schedules.
- Add a database-backed lease and idempotency key to each scheduled job.
- Preserve the current audit trail for every external write.
- Define retry, timeout, and stale-lease recovery behavior.
- If any short-lived executor remains in the web process, add a FastAPI
  lifespan shutdown hook and persist recoverable job state.

### Acceptance criteria

- Starting two web instances does not run any schedule twice.
- Redeploying during a scheduled run does not lose the run or repeat paid API
  work.
- Scheduled writes remain auditable and deterministic.
- The web process can start and stop without waiting for a long scan.

## Phase 4 — Make production releases deliberate

### Build

- Change automatic deployment from every commit to one of:
  - `checksPass` after required CI checks; recommended default, or
  - manual production promotion from a release commit.
- Prevent a second production deployment from starting while another release
  is building or promoting.
- Batch related commits behind one release commit instead of deploying every
  small push.
- Add post-deploy smoke checks for:
  - liveness;
  - readiness;
  - login page;
  - version/commit match;
  - one authenticated representative page where safe.
- Automatically stop promotion or roll back when readiness or smoke checks
  fail.
- Do not use empty commits as the normal redeploy mechanism.

### Acceptance criteria

- One approved change set produces one production deployment.
- The serving commit always matches the intended release commit.
- Failed checks cannot auto-deploy to production.
- A failed smoke test produces a clear rollback result and alert.

## Phase 5 — Add capacity only after lifecycle safety

### Recommended default

After Phases 0–4:

1. review CPU, memory, startup, and request-latency measurements;
2. upgrade Starter to Standard only if measurements show resource pressure;
3. run two Starter or Standard instances if the business requires protection
   from a single-instance restart;
4. enable autoscaling only after all recurring work is outside the web process
   or protected by cross-instance leases.

### Acceptance criteria

- Two instances pass duplicate-job and external-write tests.
- Removing either instance does not interrupt authenticated traffic.
- Capacity choice is supported by measured CPU, memory, and p95 latency.
- No scaling decision weakens Finance, Plaid, Sales, HR, or CRM auditability.

## Test and validation plan

### Automated

- Unit tests for liveness and readiness states.
- Database-down and database-timeout readiness tests.
- Pre-deploy idempotency and advisory-lock tests.
- Import test proving application construction performs no migration or bulk
  repair.
- Scheduler lease tests with two concurrent contenders.
- Graceful shutdown tests for executors and remaining tasks.
- Blueprint validation for the health path and deploy policy.

### Staging

- Ten consecutive deploys with continuous requests every second.
- Record any non-200 response from the old version until the new version is
  ready.
- Force a migration failure and prove the old version remains live.
- Force a database connectivity failure and prove readiness stays `503`.
- Start two app instances and prove each scheduled job runs once.

### Production

- Deploy during a low-risk window with no Finance/Plaid mutation testing.
- Confirm the intended commit through the readiness payload.
- Watch health, errors, CPU, memory, and restarts for 30 minutes.
- Verify login and representative Sales, Fulfillment, HR, Website Ops, and
  Finance pages.
- Confirm cron execution and audit records the following business day.

## Rollout order

1. Observability and truthful health endpoints.
2. Pre-deploy migration command.
3. Scheduler separation and idempotency.
4. Checked/manual release promotion.
5. Multi-instance or larger-instance capacity.

Do not reverse this order. Scaling first would increase cost and could multiply
scheduled work before ownership is safe.

## Rollback

- Keep the previous health endpoint during the transition.
- Keep embedded schedulers behind environment flags until external schedules
  complete one successful cycle.
- Roll back the app commit if readiness, authentication, or core page smoke
  checks fail.
- Re-enable one scheduler owner only if the external replacement fails; never
  enable both owners simultaneously.
- Database changes must remain additive and backward compatible across the old
  and new application versions.

## Decisions

### Recommended now

- Do not upgrade solely because of the observed deploy-time 502.
- Approve Phases 0–4 as the reliability fix.
- Reassess Standard or two instances using the resulting telemetry.

### Still requires confirmation

- Whether production promotion should use `checksPass` or an explicit manual
  release action.
- Whether Website Ops and outbound automation should use Render Cron or one
  dedicated worker service.
- The business uptime target: best effort, 99.9%, or higher.

Recommended defaults are `checksPass`, Render Cron for fixed schedules, and a
99.9% initial availability target.

## External references

- [Render deploy sequence and zero-downtime behavior](https://render.com/docs/deploys)
- [Render health checks](https://render.com/docs/health-checks)
- [Render uptime practices](https://render.com/docs/uptime-best-practices)
- [Render service scaling](https://render.com/docs/scaling)
- [Render instance types](https://render.com/docs/compute-plans)

