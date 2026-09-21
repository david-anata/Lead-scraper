# Outbound Pull Persistence Recovery — Fix Specification

Status: Build-ready proposal
Priority: P0 production integrity defect
Scope: StoreLeads pull persistence, Recent pulls, Company Library, saved downloads, bulk export, suppression, health, and recovery
Non-goal: changing StoreLeads filters, recipe selection, scoring, caps, or the Clay CSV schema

## Outcome

Every successful StoreLeads pull is durably and verifiably recorded before the system reports success. The same companies appear in Recent pulls and Company Library, the exact CSV can be downloaded again without another StoreLeads request, and persistence failures are visible and recoverable rather than converted into empty states.

## Verified Production Failure

On 2026-08-20, a real production Core ICP pull produced a valid CSV with 25 rows and 25 unique domains. All required Clay fields were populated. After reloading:

- Recent pulls still displayed “No pulls yet.”
- Company Library still displayed zero companies.
- There was no saved run to re-download or bulk-select.
- Company Library export produced no file while the library was empty.

The recovery source file is:

`C:\Users\DavidNarayan\Downloads\anata_clay_brands (1).csv`

SHA-256:

`17D708908F482D3199D89EE73972B72F0E4A398DA8CBC4630DF0562BDB857C7E`

## Verified Failure Mechanism

The request handler successfully runs StoreLeads, then treats persistence as best effort:

1. Failure to obtain the shared database engine is caught and replaced with `engine = None`.
2. With no engine, lead, run, membership, and delivery writes are skipped.
3. With an engine, `record_leads`, `record_run`, and `record_run_leads` catch their own exceptions and return zero.
4. The route never validates those return values or reads the records back.
5. The CSV response is returned as a normal success regardless.
6. Read functions similarly catch database/schema failures and return empty collections, causing the UI to show “No pulls yet” and zero companies instead of “Persistence unavailable.”
7. Outbound tables are created or altered during ordinary requests; failures are partially swallowed. This makes schema readiness dependent on runtime DDL permissions and request timing.
8. Current unit tests explicitly expect fail-open behavior on a bad engine and use SQLite, so they do not catch the production Postgres/Vercel failure mode.

This evidence identifies the software failure boundary. The underlying infrastructure cause—engine initialization, missing connection configuration, unavailable database, insufficient DDL permissions, or schema drift—must be confirmed through the new preflight and deployment diagnostics rather than guessed.

## Users and Risk

Primary user: the sales operator exporting ICP companies to Clay.

Business risks:

- the same companies can be pulled and contacted twice because suppression was not saved;
- a valid local CSV has no audit trail or reproducible run;
- bulk export and re-download cannot work;
- “No pulls yet” gives false confidence that the environment is healthy;
- repeated StoreLeads calls consume time/API usage without creating durable value.

## Required Design

### 1. Move schema management out of request handling

Create an idempotent outbound schema migration managed by the app’s production database initialization path. It creates or verifies:

- `outbound_contacted_domains`;
- `outbound_pull_runs`;
- `outbound_pull_run_leads`;
- `outbound_export_history`;
- `outbound_delivery_settings`;
- `outbound_delivery_history`.

The migration must use dialect-correct Postgres and SQLite definitions, named constraints/indexes, and additive compatibility steps. Ordinary GET/POST requests may verify schema version but must not perform opportunistic DDL.

Deployment fails readiness if required outbound tables/columns are absent or unwritable. Schema version is exposed in authenticated health diagnostics.

### 2. Preflight before calling StoreLeads

Before an operator pull:

1. Obtain the shared engine.
2. Execute a bounded connectivity check.
3. Verify outbound schema version and write capability without leaving test data.
4. Confirm artifact storage strategy is available.

If preflight fails, do not call StoreLeads. Return an operator-readable `503 Persistence unavailable`; show retry and diagnostic correlation ID. This prevents an untracked export and avoids wasting a source request.

Recommended default: no silent emergency fail-open. If an untracked-download override is ever required, make it a separately permissioned, explicitly confirmed incident action with a mandatory reason and audit event; do not include it in the initial fix.

### 3. Persist one pull atomically

Replace independent best-effort writes with one service operation, conceptually:

`persist_pull(engine, pull_result, recipe_snapshot, settings_snapshot, artifact_metadata, actor, idempotency_key) -> PersistedPull`

Within one database transaction:

1. Insert the run record and obtain its ID.
2. Upsert/insert each unique company into Company Library according to the existing domain contract.
3. Insert exact run membership for every unique output company.
4. Store artifact metadata: filename, SHA-256, row count, byte count, content/schema version, and storage reference.
5. Store source/query snapshot, counts, actor, environment, and timestamps.
6. Reconcile expected unique output count with run membership count.
7. Commit only when all required writes succeed.

Required invariants after commit:

- `run.fresh == unique CSV company count`;
- run-membership count equals unique CSV company count;
- every run member resolves to a Company Library company;
- artifact row count and checksum match the returned CSV;
- the idempotency key maps to exactly one run.

If any invariant or write fails, roll back the transaction. Do not create a run with missing membership or a library batch with no run provenance.

### 4. Preserve and return the artifact safely

Generate the CSV once from the finalized lead set and compute its checksum before persistence. Store the artifact durably or store enough immutable lead membership and schema version to reproduce the identical file. Preferred default: durable artifact storage plus exact membership, because it proves what the operator received.

Only return `200` with the CSV after the persistence transaction commits and a read-after-write check passes.

If the database fails after StoreLeads returns but before commit:

- keep the generated artifact in a bounded recovery store;
- respond `503` or a clearly classified partial-failure page, not an unqualified CSV success;
- provide correlation ID and `Retry persistence` action that reuses the saved payload and never calls StoreLeads again;
- expire recovery artifacts under a documented retention policy.

### 5. Make read failures distinct from empty data

Read services return a typed result with `status`, `data`, and diagnostic ID rather than converting every exception to `[]` or `0`.

UI states:

- `Ready — 0 runs`: verified query succeeded and no rows exist.
- `History unavailable`: run query/schema/database failed.
- `Library unavailable`: company query/schema/database failed.
- `Partial persistence`: a source result awaits persistence retry.
- `Ready`: run/library queries succeeded.

The words “No pulls yet” and “0 companies” may appear only after successful database reads.

### 6. Saved download and bulk export

- Per-run Download retrieves the stored artifact or regenerates it from immutable membership; it never calls StoreLeads.
- Bulk export accepts selected persisted run IDs, verifies each membership set, deduplicates by normalized domain, stores export provenance, and returns one artifact.
- Company Library `Download all for Clay` returns a valid header-only CSV when the verified library is empty, or disables the action with explicit copy. It must never silently do nothing.
- Export/download failures show a classified error and correlation ID.

### 7. Suppression truth

Keep the existing domain-normalization and never-email-twice selection behavior. A domain becomes suppressed only through the approved lifecycle event defined by the broader control-system spec. For this P0 fix, preserve current export-based suppression semantics, but ensure the write is in the same transaction and succeeds before returning the CSV.

The 25 companies from the failed production pull must be recovered into a historical run and Company Library before another production pull is performed, preventing duplicate extraction.

## API Behavior

The current GET download route is retained only as a compatibility shim during migration. Target command contract:

- `POST /admin/api/outbound/runs`: preflight, source pull, generate, persist, verify; returns run metadata and artifact location/download response.
- `GET /admin/api/outbound/runs/{run_id}`: persisted run detail.
- `GET /admin/api/outbound/runs/{run_id}/csv`: saved artifact only.
- `POST /admin/api/outbound/runs/{run_id}/retry-persistence`: authorized recovery using stored payload; never calls StoreLeads.
- `POST /admin/api/outbound/exports`: combined artifact from persisted run IDs.
- `GET /admin/api/outbound/health`: authenticated source/database/schema/artifact readiness.

All command endpoints require authorization, CSRF protection consistent with the app, idempotency key, actor capture, and correlation ID.

## Data and Migration

Add fields/tables as needed for:

- run `status`: `pending`, `persisting`, `complete`, `partial_persistence`, `failed`;
- idempotency key with unique constraint;
- actor and environment;
- recipe/settings/query snapshots;
- artifact filename, storage reference, SHA-256, bytes, row count, schema version;
- error code, sanitized error detail, correlation ID;
- timestamps for started, source completed, persisted, and failed.

Use a real migration checked into the deployment path. Do not rely on swallowed `ALTER TABLE` statements. Migration must be idempotent and tested against a snapshot representing the current production schema.

### Recovery of the verified 25-row pull

Provide a one-time authenticated recovery command that:

1. accepts the known CSV and verifies the exact SHA-256 above;
2. validates 25 rows, 25 unique normalized domains, expected header/version, and required values;
3. creates a historical `icp_baseline` run with an explicit `recovered_from_failed_persistence` note;
4. inserts Company Library rows and exact membership transactionally;
5. records the original pull time when known and recovery actor/time;
6. verifies counts after commit;
7. is idempotent by checksum and refuses a second import.

Do not infer missing scanned/matched counts. Store them as unknown/null or record only evidence-backed values.

## Observability

Emit structured events for:

- `outbound_preflight_failed`;
- `outbound_source_completed`;
- `outbound_persistence_started`;
- `outbound_persistence_committed`;
- `outbound_persistence_rolled_back`;
- `outbound_read_failed`;
- `outbound_artifact_downloaded`;
- `outbound_recovery_completed`.

Include correlation ID, run/idempotency key, recipe, counts, duration, schema version, environment, and sanitized error class. Never log credentials, contact details, full lead payloads, or artifact contents.

Alert on any persistence rollback, read failure presented to an operator, invariant mismatch, or source success without persistence commit.

## Test Plan

### Unit

- Postgres-aware schema/migration SQL and SQLite compatibility.
- Successful atomic transaction with all invariants.
- Lead, run, membership, and artifact failures each roll back everything.
- Duplicate idempotency key returns the existing run.
- Typed read result distinguishes empty from unavailable.
- Recovery checksum, row validation, and idempotency.

Replace tests that bless silent write failure for the operator command path. Low-level helpers may remain non-throwing only when callers receive an explicit failure result and must handle it.

### Integration

- Test against ephemeral Postgres, not only SQLite.
- Initialize app exactly as production does and verify the shared engine exists in the route runtime.
- Execute a mocked StoreLeads 25-row result and assert one run, 25 memberships, 25 library rows, and one artifact.
- Reload through new database connections/serverless invocations and verify persistence.
- Download twice and assert StoreLeads call count remains one.
- Combine overlapping runs and verify dedup/provenance.
- Simulate database unavailable, schema missing, permission denied, timeout, transaction failure, and read failure.

### Production smoke test

1. Confirm outbound health is Ready and schema version matches deployment.
2. Recover the known 25-row pull and verify it in Recent pulls and Company Library.
3. Download the recovered run and compare SHA-256.
4. Run one low-cap production recipe.
5. Hard reload Lead Ops and Company Library in a new browser request.
6. Verify run count, membership count, library delta, and suppression.
7. Download the saved run twice and verify no extra StoreLeads call.
8. Select the recovered and new run, bulk-export, and verify unique count.

## Acceptance Criteria

1. A successful pull is visible in Recent pulls and Company Library after a hard reload and a new serverless invocation.
2. Output count, unique domains, run membership, Company Library availability, and artifact metadata reconcile.
3. The route does not return an unqualified successful CSV when required persistence fails.
4. Database/schema failure is shown as unavailable, never as a verified empty state.
5. A saved run downloads repeatedly without calling StoreLeads.
6. Bulk export works from persisted run IDs and records provenance.
7. The exact 25-row failed pull is recovered once using its checksum and is included in suppression.
8. Production migrations run outside ordinary outbound requests and fail readiness on error.
9. The full transaction and serverless reload path pass against Postgres.
10. Existing StoreLeads filters, recipes, scoring, caps, and CSV columns remain unchanged.
11. Every failure and recovery has an operator-visible correlation ID and structured audit event.
12. No pull may report complete unless a read-after-write check confirms the run, exact membership, Company Library resolution, and artifact.

## Rollout

1. Add migration, typed health/read results, and observability; deploy with pulling temporarily disabled if health is not Ready.
2. Add atomic persistence and artifact contract behind a feature flag.
3. Run Postgres integration tests and staging serverless-reload smoke test.
4. Deploy, verify health, and recover the known 25-row pull.
5. Enable the new command path for super-admin only and run one low-cap production smoke pull.
6. Verify re-download, bulk export, Company Library, and suppression.
7. Enable for normal authorized operators; retain metrics/alerts.
8. Remove the compatibility GET execution behavior after clients/UI migrate.

Rollback disables new pulls but preserves all committed run/library/artifact data. Do not roll back schema destructively.

## Files Expected to Change

- `sales_support_agent/services/outbound_memory.py`: typed persistence/read results or replacement storage service.
- `sales_support_agent/models/database.py`: explicit outbound migration/readiness integration.
- `sales_support_agent/api/outbound_router.py`: preflight, command semantics, error states, saved downloads.
- outbound artifact/storage service if introduced.
- `tests/test_outbound_memory.py`: transaction and error-contract updates.
- route/integration tests for Postgres and serverless persistence.
- outbound operations documentation and environment/readiness guidance.

## Decision

Recommended default: block the source call when persistence preflight is unhealthy, and never issue an untracked normal export. Reliability here means preserving the source result, dedup truth, and audit trail—not merely returning a local file.
