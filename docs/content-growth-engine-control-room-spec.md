# Anata Content & Growth Engine control-room specification

Status: proposed, build-ready plan
Sources: `Anata Content & Growth Engine. Master Automation Runbook`, version
1.0, updated July 28, 2026, and `The Six C's of Building a Social Media
Audience in 2026`
Scope: authenticated Agent page, automation orchestration, and operational auditability

## Outcome

Add a Content Operations control room to Agent that answers, in one scan:

1. Is today's content system ready?
2. What is scheduled, running, delivered, blocked, or failed?
3. What did each job create or publish?
4. Which dependency needs attention?
5. What is the safest next action?

The page is an operator surface for a non-interactive automation system. It is
not a long-form editor, social composer, analytics vanity dashboard, or
replacement for Google Drive.

The automation is the product. Riverside is the primary recording and source
media system. Agent turns each eligible recording into channel-native content,
publishes it through supported runtime connectors, measures results, and uses
the strongest observed patterns to inform the next production cycle.

## Content strategy: the Six C's

Use the supplied Six C's terminology as the engine's strategy vocabulary:

1. **Channel:** choose the right destination and create for its native behavior.
2. **Credibility:** preserve David's earned experience, operating context, and
   visible action.
3. **Category:** reinforce a small, explicit set of topics, formats, phrases,
   and visual signatures.
4. **Content:** source ideas from proven demand, audience language, comments,
   questions, and real Anata operating experience.
5. **Calibration:** measure starts, retention, meaningful reactions, business
   signals, and conversion. Reuse patterns that perform, not merely posts with
   the largest view count.
6. **Collection:** direct qualified attention toward an owned audience and
   appropriate Anata offer without turning every post into a sales pitch.

The engine may learn from Codie Sanchez's terminology and the supplied research,
but it must not imitate her voice, copy her content, imply endorsement, or
present benchmark ranges as guaranteed Anata outcomes.

## Verified current state

### Agent

- Agent is a Python/FastAPI, server-rendered internal application.
- Authenticated navigation is defined centrally in
  `sales_support_agent/services/admin_nav.py`.
- Access is permission-filtered through
  `sales_support_agent/services/access/catalog.py`.
- The canonical shell is a full-width global band, a full-width section band,
  and a `1320px` constrained page canvas with `24px` desktop gutters.
- Website Ops already demonstrates scheduled-job routes, durable job leases,
  health checks, retry state, reports, queues, and run history. These patterns
  should be reused, not copied into a competing framework.
- Gmail draft and Slack integrations already exist.
- No general Google Drive, Riverside, Zapier publication, LinkedIn analytics,
  or YouTube publishing integration was found in the current Agent code.
- Website Ops is an SEO/AEO production-website system. The requested content
  engine is cross-channel editorial and distribution operations, so it should
  not be implemented as another Website Ops report.

### Runbook

The runbook defines five jobs:

| Job | Trigger | Primary output |
| --- | --- | --- |
| Daily Signal + Podcast Brief + Distribution | Daily, early morning | Google brief, topic file, Gmail draft, Slack notice |
| SEO Blog | Daily, after the brief | Published blog with image |
| Riverside Recording + Clip Harvest | Per episode | Episode assets and approved clips |
| Social Auto-Publish | After clip harvest | LinkedIn, Instagram, and YouTube publications; X staged only |
| Weekly Performance Retrospective | Monday morning | Personal LinkedIn founder reflection and retrospective record |

The runbook also establishes these non-negotiable rules:

- Google Drive is the content source of truth.
- ClickUp is not part of this workflow.
- Gmail output is draft-only.
- Every publication is logged.
- X remains staged until a supported publishing path exists.
- Weak topics are replaced rather than forced into the mix.
- Copy must connect to margin, growth, or operations, use an operator voice,
  and avoid em dashes.
- Missing integrations remain blocked, not silently treated as completed.

## Product boundary

Use two explicit sources of truth:

- **Google Drive:** canonical briefs, topic files, asset records, social queue
  documents, and retrospectives.
- **Agent database:** canonical execution ledger, schedules, dependency health,
  idempotency keys, attempts, failure details safe for operators, publication
  receipts, and audit history.

Agent stores Drive document IDs, URLs, titles, fingerprints, and operational
metadata. It should not create a second editable copy of the full content.

## Automation architecture

The engine is a closed operating loop:

`Research → Plan → Record in Riverside → Harvest → Transform by channel → Quality gate → Publish → Verify → Measure → Calibrate → Plan again`

### Riverside source contract

- Riverside is the master source for episodes, transcripts, chapters, speaker
  tracks, full video, audio, and candidate clips.
- A Riverside recording is not automatically publishable.
- Ingestion waits for required processing and records immutable provider asset
  IDs.
- Every derived claim, quote, clip, caption, and article retains its source
  episode and transcript interval.
- The engine never invents a quote, story, number, or claim absent from an
  approved source.

### Runtime connector contract

Use supported MCP or provider connectors for destination writes where those
connectors are available to the deployed Agent runtime. A connector visible only
inside a developer desktop session is not, by itself, a production integration.

Every runtime connector must support destination identity, readiness checks,
idempotent creation or upload, safe receipts, status verification, bounded
retries, and redacted structured logging. If an MCP connector cannot satisfy
those contracts, wrap the destination's supported API or an approved Zapier
action behind the same provider interface.

## Channel strategy and native playbooks

Recommended launch hierarchy:

1. **LinkedIn:** primary B2B authority and lead channel.
2. **YouTube:** primary depth, trust, and search channel.
3. **Instagram:** relationship and discovery channel.
4. **Newsletter:** owned collection channel after consent and delivery are
   approved.
5. **X:** experimental conversation channel, staging-only initially.

Store each playbook as versioned configuration. It defines the objective,
audience, eligible sources, cadence, posting windows, hook patterns, structure,
length, aspect ratio, media rules, caption rules, tone, prohibited language,
CTA policy, quality gates, metrics, observation window, and fatigue limits.

### LinkedIn

- Use tactical B2B education, operator lessons, frameworks, hiring and
  management lessons, and informed contrarian positions.
- Avoid press-release language, empty inspiration, and product promotion without
  a useful lesson.
- Measure meaningful comments, qualified profile activity, link actions, leads,
  and engagement rate.
- Company and David's personal profile use separate identities and playbooks.

### YouTube

- Use full episodes, story-driven education, cases, breakdowns, and tactical
  frameworks from Riverside.
- Produce channel-native titles, thumbnail concepts, descriptions, chapters,
  and concise openings.
- Select Shorts as independently valuable vertical moments, not arbitrary
  excerpts.
- Measure impressions, click-through rate, retention, watch time, returning
  viewers, subscribers gained, and qualified conversion.

### Instagram

- Use Reels for discovery, carousels for useful frameworks, and selected
  behind-the-scenes material for relationship.
- Keep presentation native and direct, not like a cropped corporate lecture.
- Measure watch percentage, shares, saves, non-follower reach, profile actions,
  and qualified conversion.

### Newsletter

- Convert strong relevant ideas into frameworks, operator breakdowns, and
  specific lessons.
- Never add or email a person without an approved consent contract.
- Measure delivery, opens, clicks, replies, unsubscribes, and attributed
  qualified actions. Missing tracking is unavailable, not zero.

### X

- Prepare sharp opinions, predictions, questions, and concise operating lessons.
- Stage only during the initial release.
- Never reuse LinkedIn copy automatically.

### Initial cadence

Cadence is versioned per destination. The scheduler chooses eligible assets
based on freshness, fatigue, topic balance, spacing, and recent performance. It
never publishes merely to fill a quota.

- LinkedIn personal: three strong posts per week.
- LinkedIn company: two posts per week.
- YouTube: one full episode per week plus up to three qualified Shorts.
- Instagram: up to three qualified Reels or carousels per week.
- Newsletter: one useful weekly issue after consent and delivery are configured.
- X: stage up to five candidates per week; do not publish.

These are experiments, not permanent truths. Engine-recommended cadence changes
require `content.admin` approval.

## Content intelligence and winner selection

Maintain four evidence collections:

1. **Swipe file:** public examples, hook structures, formats, reactions, and
   attribution.
2. **Problem bank:** audience questions, objections, confusion, and desired
   outcomes in the audience's language.
3. **Anata experience bank:** approved stories, decisions, frameworks, and
   lessons Anata has earned the right to explain.
4. **Performance ledger:** normalized observations for Anata publications.

External examples are research evidence only. The engine may use demand and
structural patterns but must create original substance from approved Anata
sources.

Do not define "best" as raw views. Calculate a destination-specific score from:

- **Start:** hook or click-through performance.
- **Stay:** retention, watch time, or completion.
- **Signal:** meaningful comments, shares, saves, replies, or qualified
  reactions.
- **Business impact:** owned-audience signups, qualified visits, leads, or
  attributable opportunities.
- Credibility and category fit.
- Recency and sample-size confidence.

Compare like with like: the same platform, format, objective, and observation
window. Missing signals remain unavailable.

Winner status authorizes reuse of the pattern, not blind republication. The
engine may create a follow-up from the strongest question, adapt the idea to a
different native format, deepen or shorten it, or reuse a hook structure with
new original substance. Enforce topic-fatigue, duplicate, and minimum-sample
rules.

## Selective visual generation

Visual generation is optional. Use it only when a thumbnail, diagram, carousel,
title card, or illustrative header materially improves the content. It does not
replace authentic Riverside footage or visible-action content.

Gemini may be configured as the first visual provider if David supplies an API
key and the chosen model passes commercial-use, format, rate-limit, and safety
validation. Keep the provider replaceable.

Eligible uses include YouTube thumbnails, Instagram carousel graphics, simple
framework diagrams, branded episode cards, and illustrative blog headers.

Never generate fabricated client results, warehouses, people, screenshots,
product evidence, or operating events. Never copy a creator's visual style,
misuse a likeness or trademark, or present synthetic media as documentary
evidence.

Every generated visual records its provider, model, prompt-template version,
source record, fingerprint, dimensions, brand-template version, text and claim
validation, moderation result, review state, and selected destination.

## Users and permissions

### Primary user

David, operating and auditing Anata's content engine.

### Secondary users

- Gabe or another approved operator who needs read access to daily briefs and
  distribution status.
- An administrator who manages connections, schedules, and emergency controls.

### Recommended permission model

- `content.view`: view dashboard, jobs, artifacts, publications, and safe error
  summaries.
- `content.operate`: retry failed work, run an eligible job now, and stage
  blocked output.
- `content.admin`: change schedules, connections, publishing enablement, or
  emergency stops.

Server authorization remains authoritative. Hiding navigation is not an access
control.

## Information architecture

### Recommended route and navigation

- Primary route: `/admin/content`
- Detail route: `/admin/content/runs/{run_id}`
- Optional later route: `/admin/content/settings`
- Primary navigation label: `Content`
- Section navigation:
  - `Control Room`
  - `Run History`
  - `Settings` only for `content.admin`

Recommended default: make Content a first-class product area rather than placing
it under Website Ops. Website Ops changes and measures production marketing
pages; Content coordinates briefs, recordings, assets, and multi-channel
distribution. Combining them would obscure ownership and create another
piecemealed section.

Before implementation, verify that the additional global item fits at `1280px`
and `1440px`. If it does not, solve primary-navigation overflow in the shared
shell rather than shortening labels or adding page-specific navigation.

## Page anatomy

### 1. Page header

- Eyebrow: `Content Operations`
- Title: `Content Control Room`
- Purpose: `Plan, produce, publish, and verify Anata content from one operating
  view.`
- Freshness: last successful state refresh and timezone
- Primary action:
  - `Run next eligible job` when the system is ready
  - `Resolve blocker` when a required dependency is blocked
  - no enabled action when another run already holds the lease

Manual run controls must not imply that the system requires routine human
interaction.

### 2. Operating-state summary

Use one compact decision summary, not a generic card wall:

- Today: overall state
- Next job: name and scheduled time
- Delivered: channel count versus eligible channel count
- Needs attention: blocker or failed-run count
- Weekly signal: available, unavailable, or stale

Counts must link to the filtered workspace they summarize.

### 3. Today's production line

Show the five jobs as a dependency-aware sequence:

`Signals → Brief → Riverside recording → Asset harvest → Native transformations → Quality gate → Distribution → Calibration`

Each step shows:

- state from the shared vocabulary;
- scheduled, started, and completed times;
- dependency;
- concise result;
- artifact or publication links;
- retry or resolution action when allowed.

Do not render fake progress. Jobs not applicable that day are labeled `Not
scheduled`, not `Complete`.

### 4. Command bar

- Date scope
- Job filter
- State filter
- Channel filter
- Search by topic, artifact, episode, or publication
- Result count
- Secondary `Refresh state` action

### 5. Primary workspace

Default view: a run ledger with stable columns:

- Job
- Topic or episode
- Trigger
- State
- Started
- Duration
- Outputs
- Channel result
- Owner or actor
- Next action

The table uses contained overflow, a sticky header, explicit text state, and
tabular numerals. Selecting a row opens a run detail page rather than expanding
an unbounded nested card.

### 6. Dependency health

Show the operational readiness of:

- Google Drive read and write
- Gmail draft creation
- Slack notification delivery
- Riverside episode and asset retrieval
- Zapier action keys by destination
- LinkedIn signal or analytics read
- Instagram signal read
- YouTube signal and publishing relay
- X staging-only policy

Each dependency shows `Ready`, `Blocked`, `Stale`, or `Not configured`, the
last check time, its affected jobs, and a concrete remediation action.
Credentials and raw provider errors are never displayed.

### 7. Publication evidence

For every channel write, record and show:

- destination and account;
- final state;
- provider receipt or safe external identifier;
- canonical URL when returned;
- published time;
- source artifact and run;
- content fingerprint;
- quality-gate result;
- retry count;
- failure category and recovery state.

`Requested`, `accepted by provider`, and `verified live` are separate states.

## Target workflow

### Scheduled success

1. Scheduler determines the eligible job in `America/Denver`.
2. Agent claims a durable cross-instance lease and creates a run record.
3. Preconditions and dependency health are checked.
4. The job reads its required upstream artifact.
5. The job creates or publishes the authorized output.
6. Agent records provider evidence and the Drive artifact link.
7. Postcondition verification runs.
8. The run becomes `Delivered` only when the defined delivery proof exists.

### Blocked dependency

1. Preflight identifies the missing or stale dependency.
2. No downstream external write begins.
3. The run becomes `Blocked`.
4. The control room names the dependency, affected job, and recovery action.
5. If the runbook permits staging, Agent creates the staging document and links
   it as a partial result.

### Failed execution

1. A bounded retry policy handles retryable provider failures.
2. Agent preserves the same idempotency key.
3. Exhausted retries produce `Failed`, a safe error category, attempt count,
   and a retry action for `content.operate`.
4. Downstream jobs remain blocked unless their inputs are independently valid.

### Manual run

1. Operator selects an eligible job.
2. Agent previews the job, date, dependencies, and external writes.
3. Operator confirms.
4. The scheduled-job path executes with the same lease, policy, logging, and
   idempotency behavior as an automatic run.

Manual controls cannot bypass preflight, quality gates, permissions, channel
policy, or publication logging.

## State requirements

### Loading

Render the page structure immediately. Mark only the refreshing region as
loading and announce the update through a polite live region.

### Empty

Explain whether no runs exist, no jobs are scheduled for the selected date, or
the filter returned no matches. Provide the appropriate next action.

### Partial

Name what succeeded and what remains missing. Example: the brief and Drive
topic file exist, but the Gmail draft could not be created.

### Stale

Show the age and affected decisions. Stale analytics cannot support a
performance claim.

### Error

Show a stable safe summary, retry eligibility, and support/run ID. Never show
credentials, raw response bodies, stack traces, or internal prompts.

### Permission

Keep the page shell and state explanation. Do not render hidden artifacts,
publication destinations, or restricted actions.

## Data model

Recommended relational records:

### `content_job_runs`

- `id`
- `job_key`
- `run_key`
- `trigger`
- `scheduled_for`
- `status`
- `started_at`
- `completed_at`
- `attempt_count`
- `lease_id`
- `input_fingerprint`
- `idempotency_key`
- `safe_error_code`
- `safe_error_message`
- `created_by`

Unique constraint: `(job_key, run_key)`.

### `content_artifacts`

- `id`
- `run_id`
- `artifact_type`
- `provider`
- `external_id`
- `title`
- `url`
- `content_fingerprint`
- `created_at`
- `verified_at`

### `content_publications`

- `id`
- `run_id`
- `artifact_id`
- `channel`
- `destination`
- `status`
- `provider_receipt`
- `public_url`
- `published_at`
- `verified_at`
- `attempt_count`
- `quality_gate_result`

Unique constraint: `(channel, destination, content_fingerprint)`.

### `content_source_assets`

- Riverside episode and asset IDs
- asset type and processing state
- speaker and transcript interval
- source URL and fingerprint
- ingestion time

### `content_channel_playbooks`

- channel and destination
- version and approval state
- objective and native-format rules
- cadence and quality gates
- metric contract and effective date

### `content_performance_observations`

- publication, platform, format, and objective
- comparable observation window
- available metrics and missing-metric state
- sample confidence and observed time

### `content_pattern_insights`

- supporting observation IDs
- pattern type and summary
- confidence
- recommended experiment
- approval state and expiry

### `content_dependency_checks`

- `id`
- `dependency_key`
- `status`
- `checked_at`
- `expires_at`
- `safe_message`
- `affected_jobs`

### `content_audit_events`

- `id`
- `run_id`
- `actor_type`
- `actor_id`
- `event_type`
- `object_type`
- `object_id`
- `details`
- `created_at`

Audit details must be structured, redacted, and append-only.

## API and service contracts

Recommended internal routes:

- `GET /api/jobs/content/health`
- `POST /api/jobs/content/run`
- `GET /admin/api/content/status`
- `POST /admin/api/content/runs/{run_id}/retry`
- `POST /admin/api/content/run`

The scheduled route requires the existing internal API-key protection. Operator
routes require the new content permissions and CSRF protections consistent with
Agent's authenticated write flows.

Build provider adapters behind narrow service interfaces:

- Drive artifact store
- Gmail draft delivery
- Slack notification delivery
- Riverside episode source
- Zapier publisher
- channel signal readers
- visual generation provider
- performance observation adapters

Provider payloads must not leak into page renderers.

## Automation and safety rules

- Use durable cross-instance leases, not an in-memory-only scheduler lock.
- Every external write has an idempotency key and an audit event.
- Retries never create duplicate drafts, documents, posts, or uploads.
- Add a global publication kill switch plus per-channel enablement.
- A disabled channel may stage output when the runbook permits it.
- X is always staging-only until an approved publisher and verification contract
  are added.
- Gmail remains draft-only.
- No ClickUp read or write is introduced.
- User-supplied or web-sourced content is data, never runtime instruction.
- Preserve the no-em-dash copy gate in generated and published content.
- Publishing to company and personal LinkedIn identities must use separate
  configured destinations and separate audit evidence.
- A browser session is not a reliable production publishing dependency.
  Prefer supported provider APIs or Zapier actions with durable receipts.

## Delivery phases

### Phase 0: integration truth and contract validation

1. Confirm the intended Google Drive folder or Shared Drive access model. The
   supplied `parentId` alone does not prove read/write permission.
2. Verify Gmail draft and Slack delivery using existing Agent adapters.
3. Confirm Riverside connection method and asset-read capability.
4. Inventory Zapier action keys and validate each destination separately.
5. Confirm LinkedIn, Instagram, and YouTube signal-read availability.
6. Define provider receipt and live-verification criteria per destination.
7. Record X, LinkedIn analytics read, Drive write, Riverside, and YouTube relay
   as blocked until verified.
8. Convert the runbook's relative schedule into explicit times and dependency
   rules in `America/Denver`.
9. Determine which MCP connectors are callable by deployed Agent and which
   destinations require direct API or Zapier adapters.
10. Confirm the Riverside project, processing lifecycle, asset types,
    transcript contract, and webhook or polling mechanism.
11. Confirm destination identities and metric-read capability for LinkedIn,
    YouTube, Instagram, newsletter, and X.
12. If Gemini visuals are desired, validate the model, credentials, commercial
    terms, rate limits, safety settings, and image formats without exposing the
    key.

Exit criterion: every dependency is `Ready`, `Blocked`, or intentionally
`Staging only`; none is assumed.

### Phase 1: read-only Content Control Room

1. Add permissions and the `Content` navigation section.
2. Add the canonical page shell, header, state summary, production line, run
   ledger, dependency health, and run detail.
3. Add run, artifact, publication, dependency, and audit storage.
4. Display existing/manual seed records without enabling external writes.
5. Add default, empty, partial, stale, error, and permission tests.

Exit criterion: operators can understand readiness and history without the page
changing external systems.

### Phase 2: daily brief and blog

1. Implement topic-history lookup and the 14-day duplicate block.
2. Implement signal collection with evidence labels.
3. Implement brief and topic-file creation in Drive.
4. Create Gmail drafts and Slack notices with independent delivery states.
5. Implement the SEO blog job behind explicit publishing configuration and
   production verification.
6. Enforce sequential dependency between the brief and blog.

Exit criterion: Job 1 and Job 6 are idempotent, auditable, and recover safely
from partial delivery.

### Phase 3: episode assets and social distribution

1. Implement Riverside episode and asset retrieval.
2. Preserve transcript lineage for every clip, claim, and caption.
3. Create versioned native playbooks and initial cadence configuration.
4. Transform source material separately for LinkedIn, YouTube, and Instagram.
5. Enforce channel-specific quality gates.
6. Create the episode asset record in Drive.
7. Add per-channel MCP, API, or Zapier adapters.
8. Publish eligible LinkedIn, Instagram, and YouTube outputs.
9. Stage X output only.
10. Verify and record each destination independently.

Exit criterion: one failed channel cannot create duplicates or falsely mark the
other channels incomplete.

### Phase 4: weekly retrospective

1. Add comparable weekly observation windows.
2. Keep missing analytics unavailable rather than zero.
3. Rank content using explicit, inspectable inputs.
4. Create and publish the personal LinkedIn reflection through its separately
   configured identity.
5. Write the weekly retrospective record to Drive.
6. Score comparable publications using Start, Stay, Signal, business impact,
   category fit, and sample confidence.
7. Create evidence-backed pattern insights and next experiments.
8. Feed approved patterns and audience questions into the next brief without
   copying prior content.

Exit criterion: the retrospective distinguishes observed performance,
inference, and unavailable evidence.

### Phase 5: owned audience and selective visuals

1. Add the approved newsletter destination, consent contract, unsubscribe
   safeguards, and delivery verification.
2. Transform eligible winning ideas into useful newsletter issues.
3. Add Gemini or another approved provider behind the visual interface.
4. Add brand templates, safe zones, provenance, moderation, and claim checks.
5. Enable visuals only for approved content types.
6. Compare visual experiments against authentic-footage or template-only
   controls.

Exit criterion: owned-audience writes are consent-safe, and generated visuals
cannot be mistaken for operating evidence.

### Phase 6: hardening and rollout

1. Add bounded retries, timeouts, circuit breaking, and provider-specific
   failure categories.
2. Add scheduler catch-up behavior after deploys or restarts.
3. Add alerting for overdue jobs, repeated failures, and stale dependencies.
4. Validate keyboard, focus, live regions, contrast, and reduced motion.
5. Validate desktop at `1280px` and `1440px`; mobile implementation may remain
   deferred, but global overflow is not allowed.
6. Run in shadow mode, then draft/stage mode, then enable one publishing channel
   at a time.
7. Complete production visual and operational verification after each rollout
   step.

## Analytics and operating measures

Track system quality before content performance:

- scheduled-run completion rate;
- on-time start and completion rate;
- partial-delivery rate;
- duplicate-prevention events;
- failure and recovery rate by dependency;
- time from recording available to verified distribution;
- percentage of publications with complete evidence;
- stale-signal rate.
- percentage of derived assets with complete Riverside lineage;
- playbook-compliance rate;
- winner-to-follow-up rate;
- fatigue and duplicate-prevention events;
- owned-audience growth and qualified actions when consent-safe;
- generated-visual usage and comparative experiment performance.

Channel performance remains evidence-labeled and unavailable when the required
read connection is missing.

## Accessibility and design requirements

- Use the canonical Agent shell and shared primitives from `DESIGN.md`.
- Do not introduce a second component framework or page-specific design system.
- Use Montserrat for concise headings and controls; Inter or Segoe UI for the
  run ledger and operational reading.
- Use status color only with a text label.
- Keep external URLs descriptively labeled.
- Preserve logical keyboard order and visible focus.
- Announce status refreshes and run transitions without stealing focus.
- Use a contained workspace for long tables; never introduce page-level
  horizontal scrolling.
- Prefer one production-line workspace and one ledger over nested generic cards.

## Acceptance criteria

1. An authorized operator can determine today's state, next job, next scheduled
   time, latest successful output, and exact blocker within one scan.
2. Content appears as one consistent, permission-filtered Agent product area.
3. The header bands span the full desktop viewport and align to the canonical
   `1320px` page container.
4. Google Drive remains the editable content source of truth.
5. Agent records execution and publication evidence without duplicating the
   Drive document body.
6. Every run has a unique run key, durable lease, idempotency key, attempt count,
   and audit history.
7. Every external write records its destination, fingerprint, provider result,
   and verification state.
8. A retry cannot create a duplicate Drive file, Gmail draft, Slack notice,
   blog, social post, or video upload.
9. Missing or stale dependencies block only affected work and name a concrete
   recovery action.
10. Gmail is draft-only, X is staging-only, and ClickUp is never called.
11. A job is not labeled `Delivered` merely because a request was submitted.
12. Partial runs identify exactly what succeeded and what remains.
13. Manual actions cannot bypass permissions, preflight, quality gates, channel
   policy, idempotency, or audit logging.
14. Generated and published copy passes the no-em-dash rule.
15. The page has usable default, loading, empty, filtered-empty, partial, stale,
   failed, blocked, permission, and success states.
16. At `1280px` and `1440px`, there is no clipped navigation, cut-off header
   background, obscured action, or global horizontal overflow.
17. Keyboard users can reach navigation, commands, ledger rows, and run actions
   in visual order with a visible focus indicator.
18. Existing Sales, Finance, Fulfillment, Website Ops, HR, and other business
   logic remains unchanged.
19. Every derived asset traces to a Riverside source and, when applicable, a
    transcript interval.
20. Every channel uses a versioned native playbook rather than shared cross-post
    copy.
21. Scheduling respects destination cadence, spacing, fatigue, and eligibility.
22. Winners use comparable platform-native evidence, minimum samples, and
    business relevance, not raw views alone.
23. Reuse creates a new native treatment or follow-up instead of blindly
    duplicating the winning post.
24. Every publication records its playbook version and source lineage.
25. A connector is not production-ready solely because it exists in a developer
    MCP session.
26. Generated visuals are optional, provenance-recorded, brand-checked, and
    prohibited from fabricating evidence.
27. Newsletter automation cannot write to an address without approved consent.

## Validation plan

### Automated

- Permission and navigation visibility tests.
- Route and CSRF tests for every read and write path.
- Unique-run and publication idempotency tests.
- Cross-instance lease tests.
- Provider adapter contract tests with redacted fixtures.
- Partial-success and bounded-retry tests.
- Dependency DAG tests for daily and weekly schedules.
- State renderer tests for every required page state.
- Copy-gate tests for em dashes and required topic structure.
- Riverside processing and transcript-lineage contract tests.
- Channel-playbook conformance tests.
- Cadence, spacing, fatigue, and duplicate-prevention tests.
- Comparable-window winner scoring and missing-metric tests.
- Visual provenance, claim, brand-template, and moderation tests.
- Newsletter consent and unsubscribe tests.

### Browser

- Review `/admin/content` at `1280px` and `1440px`.
- Verify the full-width global and section navigation bands.
- Verify long history tables use contained scroll and sticky headers.
- Complete keyboard-only review.
- Verify safe error and permission states do not expose restricted data.
- Confirm external artifact links identify their destination and open safely.

### Production

- Start with all publication switches disabled.
- Verify health and one shadow run.
- Enable Drive and draft/staging outputs.
- Verify one end-to-end brief run and one partial-failure recovery.
- Enable one social destination at a time.
- Confirm provider evidence and live destination before enabling the next.
- Verify scheduler catch-up after a safe restart or deployment.

## Non-goals

- Building a WYSIWYG editor inside Agent.
- Replacing Google Drive.
- Rebuilding Website Ops SEO/AEO workflows.
- Introducing ClickUp.
- Sending Gmail messages.
- Enabling X publishing without a separate approved integration contract.
- Scraping private browser sessions as the durable production runtime.
- Redesigning the public website.
- Changing unrelated business logic or permissions.
- Full mobile optimization in the first implementation phase.
- Copying another creator's voice, posts, scripts, visuals, or personal brand.
- Publishing identical creative and copy to every destination.
- Treating an unverified MCP connection as production infrastructure.
- Generating synthetic documentary evidence.

## Decisions and recommended defaults

1. **Placement:** first-class `Content` product area. Do not bury it under
   Website Ops.
2. **Source of truth:** Drive for content; Agent database for operational state.
3. **Initial release:** read-only control room and dependency truth before
   publishing automation.
4. **Scheduling:** explicit `America/Denver` times backed by Render cron or an
   equivalent durable trigger; embedded scheduling is catch-up support, not the
   only production trigger.
5. **Publishing rollout:** shadow, then stage/draft, then one destination at a
   time.
6. **X:** staging-only.
7. **Manual runs:** allowed for `content.operate`, with preview and confirmation.
8. **Failure policy:** continue independent destinations, block dependent jobs,
   and never convert partial success into complete delivery.
9. **Creation source:** Riverside is the master recording and transcript source.
10. **Channel priority:** LinkedIn for B2B authority, YouTube for depth and
    search, and Instagram for relationship and discovery.
11. **Reuse:** repeat winning structures and questions with new native creative,
    not identical cross-posts.
12. **Visuals:** optional Gemini provider behind a replaceable interface;
    authentic Riverside material remains the default.
13. **Cadence:** start conservatively and require approval for recommended
    cadence changes.
14. **Strategy vocabulary:** use Channel, Credibility, Category, Content,
    Calibration, and Collection.

## Remaining product decisions

These do not block Phase 0, but must be resolved before their affected phase:

1. Exact daily start time for Job 1 and maximum acceptable lateness.
2. Whether Job 6 publishes directly or creates a production-ready draft during
   initial rollout.
3. The authoritative destination accounts for company LinkedIn, personal
   LinkedIn, Instagram, and YouTube.
4. The exact quality-gate rules that make a clip approved for distribution.
5. Whether a failed Slack notice should block Job 1 completion or be recorded as
   a partial delivery. Recommended default: partial delivery, because the Drive
   brief and Gmail draft remain useful.
6. The safe retention period for provider receipts and generated-content
   fingerprints. Recommended default: retain audit metadata indefinitely and
   avoid storing duplicate full content in Agent.
7. Which consent-safe newsletter provider owns the list. Recommended default:
   design the contract now and enable it after the social loop is verified.
8. Whether Gemini is the preferred visual provider and which model is approved.
   Recommended default: keep the provider generic and configure Gemini first
   only after credentials and terms are validated.
9. Which visual classes require human approval. Recommended default: approve the
   first ten outputs for each template and destination, then automate only
   proven low-risk classes.
10. The approved category, three to five content pillars, repeatable formats,
    recurring phrases, and visual signatures. These are required before Phase 3.
