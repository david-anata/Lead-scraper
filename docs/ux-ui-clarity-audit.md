# UX/UI Clarity Audit - agent.anatainc.com

Date: 2026-06-29

## Executive Summary

The app is directionally strong as an operator system: the best surfaces already expose status, decision queues, and source-of-truth links. The main UX clarity issue is not visual polish. It is inconsistent language across adjacent surfaces, mixed product/system terminology, and decision labels that sometimes describe implementation state instead of operator action.

Highest-value improvements:

1. Normalize navigation and page names so operators know which surface to use.
2. Replace internal/tooling labels with action-focused labels.
3. Pair opaque IDs with human-readable context everywhere.
4. Make queue states explain what decision is required, not only what status a record has.
5. Move debug/system details below operator decisions or behind disclosure controls.

## Surface Map

Ranked by operator value and frequency:

| Rank | Surface | Primary routes | Main operator decision | Current clarity risk |
| --- | --- | --- | --- | --- |
| 1 | Sales / HubSpot | `/admin`, `/admin/sales`, `/admin/sales/deals`, `/admin/sales/deals/cleanup`, `/admin/sales/deals/{deal_id}` | Which deal needs action, what is missing, what can be safely pushed to HubSpot | Mixed names: Sales Priorities, Sales Operator, Deal Board, Cleanup Queue |
| 2 | Website Ops | `/admin/website-ops`, `/admin/website-ops/queue`, `/admin/website-ops/reports`, `/admin/website-ops/feedback/{id}` | Which website change should be approved, run, rejected, or investigated | SEO-specific labels under broader Website Ops; implementation labels leak into approval UI |
| 3 | Fulfillment CS | `/admin/fulfillment/cs`, `/admin/fulfillment/cs/reports`, `/admin/fulfillment/cs/reports/{slug}` | Which support threads need CS action, escalation, or more evidence | Report and state labels are accurate but artifact-oriented |
| 4 | Fulfillment Prospects | `/admin/fulfillment/sales` | Which prospect needs quote/cost/margin action | Good workflow density, but several labels assume internal pipeline knowledge |
| 5 | Advertising | `/admin/advertising/audit`, `/admin/advertising/clients`, `/admin/advertising/profit-calculator`, `/admin/advertising/bulk-profitability` | What spend is waste, what to scale, what file/output to use | Acronyms and upload labels are expert-oriented; ASIN context needs strengthening |
| 6 | Executive / Brand Analysis | `/admin/executive`, `/admin/executive/brand-analysis`, `/admin/executive/brand-analysis/{id}` | Which brand is attractive, risky, or ready for next-step review | Confidence/valuation language is useful but needs clearer caveats and data provenance |
| 7 | Finance | `/admin/finances/*` | What cash risk, obligation, or reconciliation needs action | Route area is broad; labels should separate sync/setup from operator decisions |
| 8 | Access | `/admin/access`, `/admin/access/users/{id}/access`, `/admin/access/requests` | Who can access what and what is missing | Mostly clear; "tool" and legacy "role" language still leaks |

## Ranked Findings

### 1. Navigation Uses Competing Names For The Same Work

Current pattern:
- Top section: `Sales Priorities`
- Subpages: `Sales Priorities`, `Sales Operator`, `Deal Board`, `Generate sales deck`
- Website Ops subpage: `SEO Dashboard`

Why weak:
- Operators must infer the difference between priority, operator, board, and cleanup.
- "SEO Dashboard" narrows Website Ops even though the page includes conversion, content, analytics, approvals, and execution.

Clearer alternative:
- Section: `Sales`
- Subpages: `Priority Queue`, `Deal Board`, `Sales Assets`
- Sales operator route label: `Sales Control Room`
- Website Ops subpage: `Overview`, not `SEO Dashboard`

Implementation notes:
- Update nav labels in `sales_support_agent/services/admin_nav.py`.
- Keep route paths unchanged.
- Add or update nav tests in `tests/test_access_rbac.py` for visible labels.

### 2. Website Ops Approval States Describe Status, Not Decisions

Current wording:
- `Active`, `Approved`, `Done`, `Error`, `Rejected`
- `Approve and Execute`
- `Submit Review`
- `Action value`
- `Target post ID`

Why weak:
- "Active" does not say the item needs review.
- "Approved" does not say whether it is safe to run, queued to run, or already running.
- "Action value" and "Target post ID" are implementation terms.
- "Approve and Execute" sounds immediate even when execution depends on auto-execution settings and supported action types.

Clearer alternative:
- `Active` -> `Needs review`
- `Approved` -> `Approved to run`
- `Done` -> `Completed`
- `Error` -> `Failed`
- `Approve and Execute` -> `Approve safe action`
- `Submit Review` -> `Save decision`
- `Action value` -> `Exact content or change`
- `Target post ID` -> `WordPress page ID`

Implementation notes:
- Update status label mapping and queue filter labels in `sales_support_agent/services/website_ops.py`.
- Preserve underlying status values: `new`, `approved`, `in-progress`, `done`, `error`, `rejected`.
- Add render tests for the queue and feedback detail labels.

### 3. Website Ops Dashboard Mixes Operator Decisions With System Details

Current pattern:
- Hero: `SEO control tower`
- Secondary card: `Current scope`
- Lower cards: `Customer Questions`, `SERP Blueprints`, `Content Tasks`, `Data connection notes`, `System details`
- Debug panel label: `MVP debug`

Why weak:
- The dashboard is broader than SEO; "control tower" is metaphorical and less concrete than "action center".
- Debug/system sections are useful but compete with the queue and recommendations.
- `SERP Blueprints` is jargon for operators who need to know "what ranking pages are doing".

Clearer alternative:
- H1: `Website action center`
- Lead: `Review what changed, what needs approval, and which website actions are safe to run.`
- `Customer Questions` -> `Buyer questions to answer`
- `SERP Blueprints` -> `Search patterns from ranking pages`
- `Content Tasks` -> `Recommended content updates`
- `Data connection notes` -> `Data sources`
- `MVP debug` -> `Generation trace`

Implementation notes:
- Update only display copy first.
- Keep the current layout and data sources.
- Move system/debug cards after action queue/report cards where feasible.

### 4. Sales Surfaces Need A Clearer Page Taxonomy

Current wording:
- `Sales Priorities - HubSpot`
- `Deal Board`
- `Cleanup Queue`
- `Run cleanup ->`
- `Apply selected`
- `Fix in HubSpot`
- `hygiene flags`

Why weak:
- "Cleanup" sounds low-value, but the page applies revenue-critical HubSpot fixes.
- "Apply selected" does not say where changes go.
- "Hygiene" understates missing contacts, line items, amounts, stale dates, or follow-up blockers.

Clearer alternative:
- `Cleanup Queue` -> `HubSpot Fix Queue`
- `Run cleanup ->` -> `Review HubSpot fixes`
- `Apply selected` -> `Apply selected HubSpot updates`
- `hygiene flags` -> `manual HubSpot fixes`
- Deal board intro should keep the strong "essentials needed to close" language.

Implementation notes:
- Update labels in sales deal board and batch/cleanup renderers.
- Keep "Open in HubSpot" and "Fix in HubSpot" links because they correctly indicate source of truth.
- Add tests around cleanup page CTAs if render tests exist; otherwise add a narrow string-render test.

### 5. Entity Context Should Come Before IDs

Current pattern:
- Routes and forms use deal IDs, report slugs, target post IDs, run IDs, and ASINs.
- Some pages already pair deal names and HubSpot links well.
- Advertising/deck flows include ASIN data but need stronger listing-title pairing.

Why weak:
- IDs are useful for systems, not first-pass operator comprehension.
- Operators need to verify they are acting on the correct deal, brand, listing, page, or report before taking action.

Clearer alternative:
- Show `Name / title` first, ID second.
- Format examples:
  - `Ocean Rx Experience Pure Blue Spirulina - B0TARGET01`
  - `Acme Wholesale - HubSpot deal 123456`
  - `Fulfillment CS Review - 2026-06-29 report`
  - `Services / Fulfillment - WordPress page 42`

Implementation notes:
- Apply first to forms/tables where actions are taken: Sales create/detail, Website Ops feedback detail, Advertising audit outputs, Brand Analysis history, Fulfillment reports.
- Do not remove IDs; demote them to secondary text.

### 6. Fulfillment CS Is Artifact-Oriented Instead Of Action-Oriented

Current wording:
- `Fulfillment CS`
- `Artifact-driven visibility into fulfillment support candidates, state, and escalation needs.`
- `Candidate threads`
- `Lifecycle states`
- `Action recommendations`

Why weak:
- "Artifact-driven" and "candidate" are internal processing terms.
- The page should immediately tell CS what to answer, escalate, or investigate.

Clearer alternative:
- Intro: `Review support threads that need an answer, escalation, or more evidence.`
- `Candidate threads` -> `Support threads`
- `Lifecycle states` -> `Thread status`
- `Action recommendations` -> `Recommended CS actions`
- `Ready to answer` should remain; it is clear.

Implementation notes:
- Update display copy in fulfillment dashboard renderer.
- Preserve data model and report artifacts.
- Add tests for empty-state copy where existing tests check rendered fulfillment pages.

### 7. Advertising Upload Copy Assumes Expert Knowledge

Current wording:
- `Drop all your Amazon exports here`
- `Bulk file, Search Term, Business Report, SQP, DSP - in any order`
- `External marketing spend - off-Amazon channels for blended TACoS`
- `Run audit & build burn list`
- Advanced labels: `Per-ASIN COGS`, `Brand Analytics SQP`

Why weak:
- Acronyms are useful for advertising operators but should be paired with plain-language descriptions.
- "Burn list" is vivid but less precise for review/approval workflows.

Clearer alternative:
- `Drop all your Amazon exports here` -> `Upload Amazon performance files`
- Helper: `Bulk Operations, Search Term, Business Report, Search Query Performance, and DSP files. The tool identifies each file automatically.`
- `External marketing spend` -> `Off-Amazon marketing spend`
- `Run audit & build burn list` -> `Build recommendations`
- `Per-ASIN COGS` -> `Product costs by ASIN`

Implementation notes:
- Update display copy only in the advertising audit page first.
- Keep acronym in parentheses after the plain label where operators search by acronym.
- Add an ASIN display rule to show title/brand where data exists.

### 8. Access Management Still Leaks Implementation Terms

Current wording:
- `tools`
- `People -> Manage access`
- Catalog label: `Access admin (users & roles)`
- Some routes still expose roles pages while UI says per-person access.

Why weak:
- "Tools" is acceptable internally but less precise than "access".
- "users & roles" conflicts with the current per-person access model.

Clearer alternative:
- `tools` -> `app areas`
- `Access admin (users & roles)` -> `People and access`
- `No access` page:
  - Current: `You don't have access to <tool> yet.`
  - Better: `You are signed in, but this app area is not enabled for your account. Ask an admin to turn on <area>.`

Implementation notes:
- Update access catalog label and forbidden page copy.
- Keep permission keys unchanged.
- Retain role routes for compatibility unless a separate cleanup thread removes them.

## Follow-Up Implementation Threads

### Thread 1 - Navigation And Access Labels

Goal:
- Normalize app-wide names without changing routes or permissions.

Files:
- `sales_support_agent/services/admin_nav.py`
- `sales_support_agent/services/access/catalog.py`
- `sales_support_agent/services/access/pages.py`
- `tests/test_access_rbac.py`

Acceptance criteria:
- Nav shows `Sales`, `Priority Queue`, `Sales Control Room`, `Deal Board`, `Sales Assets`, `Website Ops`, `Overview`, `Queue`, `Reports`.
- Access denied page uses "app area" language.
- RBAC route behavior is unchanged.

### Thread 2 - Website Ops Decision Language

Goal:
- Make Website Ops read as a decision/action queue, not a technical dashboard.

Files:
- `sales_support_agent/services/website_ops.py`
- `tests/test_admin_website_ops.py`

Acceptance criteria:
- Queue filters read `Needs review`, `Approved to run`, `Completed`, `Failed`, `Rejected`.
- Approval buttons distinguish safe action approval from manual review.
- Debug/system labels are softened and lower priority.
- No status enum or API payload values change.

### Thread 3 - Sales / HubSpot Action Clarity

Goal:
- Clarify where operators review deals, apply HubSpot updates, and fix manual blockers.

Files:
- `sales_support_agent/services/sales/deal_board.py`
- `sales_support_agent/services/sales/deal_batch.py`
- `sales_support_agent/services/sales/operator_dashboard.py`
- relevant sales render tests

Acceptance criteria:
- "Cleanup" is replaced with "HubSpot Fix Queue" language.
- CTAs say whether they apply updates to HubSpot.
- Manual flags are labeled as manual HubSpot fixes.
- Deal/company/contact names remain primary; IDs remain secondary.

### Thread 4 - Fulfillment CS Report Clarity

Goal:
- Shift Fulfillment CS from artifact language to CS action language.

Files:
- `sales_support_agent/services/fulfillment_dashboard.py`
- `tests/test_main_fulfillment_cs.py`

Acceptance criteria:
- Page intro and cards answer: what needs an answer, escalation, or investigation.
- "Candidate" labels become "support thread" labels.
- Report slugs are secondary to report titles/dates.

### Thread 5 - Advertising / Brand / Finance Form Clarity

Goal:
- Make data upload and analysis forms easier to scan and safer to act on.

Files:
- `sales_support_agent/services/advertising/audit_page.py`
- `sales_support_agent/services/brand_analysis/report_page.py`
- finance renderer modules under `sales_support_agent/services/cashflow/`

Acceptance criteria:
- Upload labels explain each file in operator language.
- Acronyms are paired with plain-language labels.
- ASIN references show listing title plus ASIN wherever title data is available.
- Form CTAs say the output produced, not the internal process.

## Practical UX Rules To Apply

1. Use action labels, not process labels: `Review HubSpot fixes`, not `Run cleanup`.
2. Show human context before IDs: title/name first, ID second.
3. State the decision required: `Needs review`, `Approved to run`, `Manual fix required`.
4. Keep source of truth explicit: `Edit in HubSpot`, `WordPress page`, `Agent report`.
5. Use progressive detail: operator action first, system/debug details below.
6. Preserve expert acronyms only after plain labels.
7. Empty states should explain why nothing appears and what creates the first item.

## Validation Plan

For each implementation thread:

- Run focused render tests for the touched surface.
- Run `python3 -m pytest tests/test_access_rbac.py` after nav/access label work.
- Run `python3 -m pytest tests/test_admin_website_ops.py` after Website Ops label work.
- Browser-check authenticated pages on desktop width:
  - `/admin`
  - `/admin/sales`
  - `/admin/sales/deals`
  - `/admin/website-ops`
  - `/admin/website-ops/queue`
  - `/admin/fulfillment/cs/`
  - `/admin/advertising/audit`
  - `/admin/executive/brand-analysis`
- Confirm no route paths, permission keys, status values, or data payloads changed unless explicitly planned in a later engineering thread.

