# Website Ops ranking system specification

## Outcome

Website Ops is the autonomous SEO and AEO operating system for production
marketing pages on `anatainc.com`. It collects trustworthy evidence, identifies
ranking opportunities, validates eligible corrections, publishes approved
low-risk changes, verifies production, rolls back invalid releases, and emails
`david@anatainc.com` only when meaningful state changes.

## Operational contract

- Scheduled runs start at 8:00 AM `America/Denver`.
- Production marketing pages are the only writable scope.
- Missing source data is unavailable, never numerical zero.
- Runtime, crawl, decision-data, publishing, and production-verification health
  are separate states.
- Simulated AEO prompts are hypotheses, not observed demand.
- Every action records its evidence, reason, confidence, risk, validation, and
  rollback reference.
- No automatic page deletion, URL migration, redirect, robots, noindex, or
  canonical change is allowed until a dedicated high-risk policy is shipped.
- Gmail customer language remains quarantined until relevance filtering,
  quoted-text removal, signature removal, PII redaction, and retention controls
  are verified.

## Delivery phases

### Phase 0 — operational truth

Validate Google credentials and configured properties, expose decision-data
readiness in health, preserve missing-versus-zero semantics, suspend
ranking-led recommendations while evidence is unavailable, clarify AEO labels,
and quarantine unsafe customer-language evidence.

### Phase 1 — production inventory

Join sitemap, rendered crawl, repository routes, GSC landing pages, GA4 landing
pages, and optional Screaming Frog imports. Record requested, final, and
canonical URLs; discovery sources; scope; raw/rendered differences; crawl depth;
and orphan candidates. Separate seed, discovered, canonical, excluded, failed,
and healthy counts.

### Phase 2 — one page, one intent

Give each canonical marketing page one primary intent, audience, funnel stage,
supporting topics, and exclusions. Cannibalization requires query overlap plus
content or metadata evidence. Low-confidence semantic similarity cannot trigger
consolidation or canonical changes.

### Phase 3 — validated autonomy

Low-risk metadata, internal-link, alt-text, and visible-content schema
corrections may autopublish after scope, intent, claim, diff, build, rendered
preview, deployment identity, and production recrawl gates pass. Higher-risk
changes stay automatically blocked until dedicated policies exist.

### Phase 4 — deployment verification and rollback

Each action has a deterministic fingerprint. Production verification confirms
the expected commit, response, indexability, canonical, rendered content, links,
and schema. Failed verification reverts only the responsible change, verifies
recovery, records the rollback, emails David, and may disable the failing action
class.

### Phase 5 — outcome learning

Compare suitable pre/post GSC and qualified GA4 conversion windows without
claiming causation. Use observed outcomes to tune prioritization and retire
action classes that repeatedly fail to deliver value.

## Acceptance criteria

1. Malformed Google credentials make health `blocked`.
2. A failed GSC or GA4 connection cannot produce zero-valued performance cards,
   page scores, or ranking-led content actions.
3. A successful zero-row response remains distinguishable from unavailable
   data.
4. Crawl health cannot imply decision-data readiness.
5. Simulated prompts never appear as search or customer evidence.
6. Out-of-scope hosts cannot enter the publishing pipeline.
7. Every published change is tied to a commit, deployment, production check,
   and rollback reference.
8. Unchanged successful runs remain in the ledger without sending email.
9. The dashboard identifies the blocking source and the concrete next action.
10. No raw or unrelated Gmail material appears in Website Ops.
