# Website Ops: Simple Command Center

Status: Proposed redesign  
Date: 2026-08-11  
Primary user: David Narayan  
Scope: `agent.anatainc.com/admin/website-ops`

## The outcome

Website Ops should answer four questions in under ten seconds:

1. Is the website program working today?
2. What reached production?
3. What happens next?
4. Does David need to do anything?

The refreshed system keeps the existing evidence, safety checks, history, and
rollback controls. It removes operational jargon and moves detailed ledgers
behind simple summaries.

## Current-experience audit

### What works

- The system preserves evidence and does not automatically trust crawler
  warnings.
- Production changes, reports, indexing evidence, and user decisions are
  retained.
- Manual sweep controls and history are available.
- The responsive layout does not create horizontal overflow.

### Highest-impact problems

#### 1. The first screen contradicts itself

The overview says "Continuous optimization is ready" while the same page says
the latest daily sweep failed, the daily portfolio is 0 of 8, and work remains
blocked or unverified. A readiness claim is not a useful answer to "is this
working?"

Desired behavior: one truthful top-level state based on production outcomes:

- **On track:** verified production work completed today.
- **Working:** a run is active and has not exceeded its normal window.
- **Needs attention:** a run failed, verification failed, or required work is
  blocked.
- **Quiet day:** no safe work qualified after the full candidate review.

#### 2. The interface reflects the old architecture

The page still describes Agent as researching, publishing, and running hourly
pulses. The current architecture is different: Codex performs the weekday
research, writing, publication, deployment, and production verification;
Website Ops inventories the site, supplies evidence, measures outcomes, and
reports.

Desired behavior: name the two responsibilities once in plain language and do
not show disabled model-API citation testing as a blocker.

#### 3. Too many pages expose the same workflow from different angles

Overview, Indexing, Query Map, Strategy, Candidates, Queue, and Reports require
the user to understand internal stages before finding an outcome. The Overview
alone contains 20 secondary headings, 30 article-like blocks, and more than
4,000 pixels of vertical content on desktop. Indexing exceeds 38,000 pixels.

Desired behavior: four destinations organized around user questions, not
internal data structures.

#### 4. Counts are large but not actionable

Examples include 118 crawl warnings, 51 indexing URLs, 329 active qualified
items, 590 validating items, 169 URLs without an intent owner, and 103 orphan
candidates. These numbers appear without a concise statement of what is real,
what is merely suspected, and what will happen next.

Desired behavior: every count belongs to one of three plain groups:

- **Confirmed problem**
- **Being checked**
- **No action needed**

Only confirmed problems should create work or appear as a warning.

#### 5. David's work is mixed with system information

The current to-do list includes a system ownership statement about Codex. That
is not an action David can take.

Desired behavior: "Needs you" contains only a decision or action that cannot be
completed safely without David. When empty, say "Nothing needs you."

#### 6. Mobile is technically responsive but too long

The mobile overview is almost 8,000 pixels tall. The introductory cards consume
most of the first screen before the user sees today's result.

Desired behavior: the first phone viewport shows today's state, production
output, and any action for David. Supporting detail is collapsed by default.

## Proposed information architecture

### 1. Today

The default command center.

Shows:

- One truthful status sentence.
- Today's production result: articles published, other verified changes, and
  the daily article goal.
- The latest verified production URLs.
- What is running now and what happens next.
- A single "Needs you" card, shown only when required.
- Last successful run and next scheduled run.

Replaces most of the current Overview and Queue.

### 2. Content

Shows the publishing program without exposing implementation machinery.

- Live article count and articles published today.
- Four service-pillar balances.
- Candidate pool grouped as Qualified, Being researched, Rejected, and
  Published.
- One-page-one-intent conflicts in human language.
- Existing-page improvements and FAQ work.
- Direct links to each production page.

Replaces Strategy, Query Map, and the content portion of Candidates.

### 3. Site health

Shows only verified website problems and the checks still in progress.

- Confirmed issues by category: broken links, indexing, metadata, structured
  data, image descriptions, and redirects.
- Items being checked, clearly labeled as unconfirmed.
- Affected-page list only after a category is opened.
- Last crawl, sitemap URL count, and last successful verification.
- Rollback availability for recently changed pages.

Replaces Indexing and the technical portion of Candidates.

### 4. History

- Daily outcome cards.
- Production changes and their URLs.
- What failed and how it recovered.
- Search Console and GA4 movement, with the measurement period visible.
- Emails sent.
- Expandable evidence and audit details.

Replaces Reports and the audit-heavy portions of the current Overview.

## Target Today screen

### Header

**Website growth**  
Plain-language subtitle: "Codex publishes. Website Ops verifies and measures."

Primary action: **Run today's plan**  
Secondary action: **View latest report**

Weekly and maintenance controls move into an overflow menu labeled "More."

### Status card

Example after the proven canary:

> **On track**  
> 1 article reached production today. All 19 live articles passed the latest
> availability check.

The card must never say ready when the latest required outcome failed.

### Today's publishing

- **1 published**
- **7 remaining toward today's goal**
- **19 live articles**
- Four compact pillar bars with published and qualified counts.

Copy beneath the goal:

> The goal is up to eight qualified articles. Unsafe or duplicate topics are
> skipped, not forced.

Each newly published article appears as one row with title, pillar, status,
production link, and verification time.

### Next

Show no more than three items:

- **Now:** the active operation.
- **Next:** the next qualified operation.
- **Later:** the next maintenance priority.

Each item gets one sentence explaining why it matters. Confidence, risk,
executor coverage, and validation conditions remain available under "Details"
but are not displayed by default.

### Needs you

One card at most on the main screen.

Example:

> **Confirm lead tracking**  
> Submit one real service-page inquiry so Website Ops can confirm that qualified
> leads are measured correctly.  
> **Open instructions**

System notes, ownership statements, and unavailable optional data never appear
here.

## Plain-language vocabulary

| Current wording | New wording |
| --- | --- |
| Continuous optimization | Website growth |
| Daily sweep | Today's plan |
| Action portfolio | Today's work |
| Candidate ledger | Possible improvements |
| Query Map | Page topics |
| Indexing remediation | Pages missing from search |
| Crawl warning | Possible site issue |
| Production delta | Published change |
| Validated autopush | Safe automatic publishing |
| Citation testing blocked | Answer-engine evidence unavailable |
| Intent owner | Page responsible for this topic |
| Canonical conflict | Two pages competing for the same topic |

Technical terms may remain in expandable evidence for expert troubleshooting.

## Important states

### Loading

Show the last known result immediately with "Refreshing" beside its timestamp.
Do not replace the page with empty skeletons.

### Working

Show the current step, start time, and expected next check. Disable the run
button and label it "Running."

### Successful day

Lead with verified production output and links. Never count generated, queued,
committed, or deploying work as published.

### No qualified work

Use "Quiet day," not success or failure. Show how many candidates were reviewed
and the top rejection reasons. This state is permitted only after the complete
candidate review required by the publishing routine.

### Failure

State where work stopped in plain language, what remained unchanged, whether
rollback was needed, and when the system will retry. A failed run overrides any
generic readiness message.

### Stale evidence

Label the affected section "Last checked [time]." Stale Search Console, GA4,
crawl, or indexing data must not make the entire system appear healthy.

### Permission

Hide controls the user cannot run. Show read-only outcomes without exposing
internal permission names.

### Mobile

The first 844-pixel viewport must include status, today's output, and "Needs
you." Navigation becomes a four-item menu. Detail tables become stacked rows.

## System behavior

### Codex owns

- Daily topic discovery and qualification.
- Source-backed writing.
- Deterministic content repairs.
- Repository validation.
- Deployment and production verification.

### Website Ops owns

- Sitemap-backed production inventory.
- Page-topic ownership evidence.
- Search Console and GA4 evidence.
- Verified technical issue tracking.
- Outcome history and the once-per-workday report.

### Shared contract

After Codex verifies a publication, Website Ops must reconcile it on the next
run and display it as published. The shared record includes URL, title, pillar,
primary topic, publication time, verification time, sources present, blog-link
check, sitemap check, and deployment reference.

Website Ops must not depend on an Agent model API key. Disabled API-based
research is an intentional configuration, not a blocker.

## Acceptance criteria

1. David can identify today's state, production output, next operation, and any
   required action without scrolling on a standard desktop display.
2. The same four answers appear within the first phone viewport at 390 by 844.
3. The main navigation contains Today, Content, Site health, and History.
4. The overview contains no more than three forward-looking work items.
5. A failed required outcome cannot coexist with an "On track" or "Ready"
   headline.
6. Agent's empty article queue cannot be presented as a publishing blocker.
7. API-based citation testing is shown as intentionally off; answer-engine
   evidence is labeled unavailable when absent.
8. Generated, queued, committed, deploying, published, and indexed states remain
   distinct in data, but the main UI highlights only published and indexed.
9. Every published article links to production and shows its verification time.
10. Unverified crawler warnings never appear as confirmed issues.
11. "Needs you" contains only actions David can perform or decisions only David
    can make.
12. Every metric displays its measurement period or "period unavailable."
13. Existing audit history, rollback records, production inventory, candidate
    evidence, and report records remain available after migration.
14. Desktop and phone views have no horizontal overflow, clipped controls, or
    essential information available only on hover.

## Rollout

1. Correct state language and architecture ownership on the existing Overview.
2. Add the shared publication record and reconcile the 19th live article.
3. Build the Today screen using existing records.
4. Consolidate the seven current destinations into Content, Site health, and
   History while preserving old URLs as redirects or compatibility routes.
5. Move technical ledgers under expandable details.
6. Validate against one successful publication day, one quiet day, one failed
   deployment, one stale-data day, and one user-decision blocker.
7. Release to production and complete desktop and phone acceptance testing.

## Recommended defaults and open decisions

- **Recommended:** David is the primary audience; specialist evidence remains
  available but is not the default view.
- **Recommended:** keep the target of up to eight qualified articles, while
  presenting production truth rather than treating eight as a guaranteed
  quota.
- **Recommended:** one workday email after the publishing and reconciliation
  cycle finishes.
- **Decision to confirm:** should "Run today's plan" be available only to David,
  or to every Website Ops administrator?
- **Decision to confirm:** should the default History view emphasize published
  work or organic-lead measurement? Recommended default: published work until
  lead attribution becomes trusted.
