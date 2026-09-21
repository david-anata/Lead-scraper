# Vercel comments: consolidated implementation specification

Status: proposed; actionable requirements with explicitly gated business decisions.
Prepared: September 10, 2026.
Scope: all accessible Vercel Toolbar comments in the Anata team, across projects and branches.
Companion: [complete comment ledger](vercel-comments-review-ledger-2026-09-10.md).

## 1. Outcome and users

Operators can find records, understand readiness, complete work without losing context, and trust the numbers and integration states they see. Prospects can select the correct product, understand approved pricing, and submit inquiries successfully.

Primary users are Agent operators and AEs, Building sales/event staff and visitors, Onyx staff and account holders, and Anata/Ascend website visitors. Each project retains its own application, brand, authorization and deployment boundaries.

This is a specification request. No application implementation, deployment, external message, comment reply, or comment resolution is part of this deliverable.

## 2. Review coverage and evidence

| Vercel project | Owning repository | Open | Resolved |
| --- | --- | ---: | ---: |
| anata-agent-staging | david-anata/Lead-scraper | 66 | 0 |
| anata-website | david-anata/anata-website | 4 | 209 |
| anata-building | david-anata/anata-building | 23 | 0 |
| onyx-bio | david-anata/onyx-bio | 53 | 24 |
| ascend-companies | david-anata/ascend-companies | 5 | 0 |
| Total | Five repositories | 151 | 233 |

Retrieved 384 unique threads and 542 messages. Both status lists were paginated to their end without project, branch, or date filters. The returned message counts match the complete message arrays. The other four projects in the team returned no threads in this inventory.

Evidence levels used here:

- **Current source evidence:** Agent People renders requests, invites and users in one table without a search control in the inspected render function, in `sales_support_agent/services/access/pages.py`.
- **Current source plus public-page corroboration:** `../anata Website/src/content/anata-intelligence.ts` defines `INTELLIGENCE_APP` as the Shipping OS host. The retrieved [Intelligence page](https://anatainc.com/platform/anata-intelligence) also contains plan CTAs to that host while its login distinguishes Intelligence. The retrieved page was crawled five days before review; recheck the exact deployment during implementation.
- **Public-page evidence:** [Pricing](https://anatainc.com/pricing) already exists and appears in navigation. Extend it for the new request instead of creating a duplicate route. Text extraction does not verify interactive tab contents.
- **Historical feedback:** all other defects are reported observations tied to the comment's date, selector and deployment context. They are not claimed as freshly reproduced failures.
- **Local planning context:** related Agent specs and uncommitted implementation changes already exist. Their presence is not deployment evidence.

The Arena page could not be retrieved through the browsing tool; that tool failure is not evidence of an application outage. Comment screenshots were not visually inspected. For vague feedback such as “this doesn't work,” use the stored selector and original screenshot before deciding the fix. The ledger links every thread and retains available selection context.

## 3. Scope, priorities and constraints

P1: broken journeys, misleading data, missed scheduled work, lost submissions, contract/invoice failures, and incorrect product routing.
P2: search, table controls, editing, understandable integration setup, approved pricing presentation and account workflows.
P3: typography, spacing, images, and restrained motion.

Include every open comment through the workstream mapping in the ledger. Duplicate comments share one implementation but retain separate verification references. Resolved comments are regression history, not an automatic backlog.

Non-goals: a rewrite of all five applications, framework migrations driven by pasted component prompts, inventing prices or credentials, implementing Shopify parity in one release, changing unrelated lead-selection rules, or reopening every resolved thread.

Agent work follows AGENTS.md and DESIGN.md: Python/FastAPI, isolated integration services, audited external writes, canonical shell, progressive enhancement and truthful evidence classes. Other repositories must use their own current working rules and design authority before implementation. A pasted React/Shadcn component is a visual reference, not authority to install a framework into Agent.

Preserve existing routes and downstream contracts unless a specific migration below requires a compatible extension. Keep existing local changes out of unrelated commits. Do not infer a production host solely from the Vercel project name.

## 4. Agent workstreams

### A1 — People search and a simpler sign-in page · P2

Sources: 1yHQ1zont5ck, -2qnxs77fiOE.

Add a labeled Search people input above the existing unified People table. Match name and email case-insensitively across requests, invitations and provisioned users. Update results while typing, show matching/total counts, and provide Clear and No matching people states. Keep row identity and access actions unchanged. For the currently fully rendered set, client filtering is sufficient; any future pagination must search the full authorized dataset server-side.

Simplify login to the sign-in action, necessary help and useful errors, retaining the existing authentication flow.

Acceptance: typing part of a name or email narrows all three record types; clearing restores the authorized set; keyboard focus stays in search; an empty result does not imply no accounts exist; no action gains permissions through filtering.

Affected area: access/pages.py, access_router.py and the existing login renderer.

### A2 — Trustworthy, presentable sales decks · P1 then P3

Sources: all 21 open deck threads, including DJ5No3MVbzcs, vfB9HeasHzVD, GAwHO8YVhuvO, BZQA-3ZzO1V2 and the repeated spotlight-card references.

Reconcile against [sales-deck integrity and economics](sales-deck-data-integrity-and-economics-spec.md) before coding; it is a proposed existing spec, not proof the work shipped. The recent “Dixie Belle Women” comments must not be conflated with the furniture-paint example in that document.

1. Verify target ASIN, variation, currency, price, time window and competitor eligibility before comparing figures. Record source and freshness.
2. Present a price and its signed delta in distinct labeled fields. The observed “$9.50-$2.49” must not read like a reversed price range.
3. Calculate estimated competitor sessions as revenue / price / CVR, using 0.15 only as an explicitly labeled planning assumption. Example: $15,000 / $20 / 0.15 = 5,000 estimated monthly sessions. Missing or nonpositive price/CVR yields unavailable, not infinity or zero traffic.
4. Reconcile channel totals with the same scenario, months and evidence set; explain conversion, spend and contribution assumptions. Unsupported claims remain Needs review.
5. Put interpretation above the full-width comparison table; reduce the title column while preserving full titles through accessible disclosure. Keep all KPI labels/values legible, including 30-day revenue.
6. Enlarge product images in a keyboard-accessible dialog with Escape and focus return; fit images according to product versus lifestyle content.
7. Use verified platform/service assets. Phase labels read “Months 1–3” without “Activates.”
8. Adapt the repeated card emphasis request to the existing renderer and brand tokens. Hover/focus may emphasize the card without obscuring its numbers or adding a dependency.

Acceptance: reproduce reported comparisons from sanitized fixtures; manually reconcile price, revenue, sessions and channel totals; block unsupported readiness; test long values at 1280/1440px, presentation size, phone and print/export. Include zero, missing, stale and wrong-variation fixtures.

Affected area: deck/dataset.py, service.py, growth_plan.py, formatting.py, rendering.py, story.py and applicable deck tests. Existing modifications need reconciliation rather than replacement.

### A3 — Outbound scheduling, recipes and operational tables · P1/P2

Sources: 17 open outbound threads. Dependencies: [daily lead batch operating system](outbound/20-daily-lead-batch-operating-system-spec.md) and [pull persistence recovery](outbound/19-pull-persistence-recovery-spec.md).

Unify recipe definition and weekday scheduling in one editable recipe workspace. Use a human name; generate the stable internal key. Include add/edit actions, recipe width, created date, header sorting/filtering, readable timestamps and paginated history.

Recommended table default: 25 rows, with 10/25/50 choices, full-set result count, stable sorting and filters preserved across pages. Apply it to recent pulls, change log and lead lists; pagination must never hide inaccessible tail records.

Provide an edit-time dialog using America/Denver and show the next scheduled run with zone abbreviation. Record schedule version and audit history. DST transitions must follow local wall-clock time. A due run appears as queued/running/completed/failed/skipped with a reason; “zero leads” cannot stand in for “job never executed.”

Show Instantly readiness with a concrete setup or configuration action appropriate to the real adapter, last successful check and actionable failure. Do not invent an OAuth flow if the integration uses a stored API credential. Show the configured Slack destination in recognizable form; changes require the existing administrative capability and audit.

Group tuning fields by purpose, display units and validation, and save together with a readable change summary. Preserve StoreLeads/Apollo behavior and CSV contracts.

Acceptance: one scheduled local-time run creates one durable batch; retries do not duplicate it; a missed run exposes cause and recovery; schedule changes persist and recalculate next-run time; a 51-row fixture remains entirely reachable; sorting is across the result set. Validate connection failures without sending real campaigns or Slack messages.

Affected area: outbound_router.py, outbound_jobs.py, outbound_settings.py, outbound_memory.py and existing job configuration.

### A4 — Building CRM, calendar, contracts and billing · P1/P2

Sources: 20 open Agent Building threads. Reconcile with [Building control-room completion](anata-building-admin-control-room-completion-spec.md) and [contract workspace](building-contract-workspace-spec.md).

Show all inquiry types in one filterable table with customer, inquiry/booking name, dates, source and progress. Preserve raw submissions in detail disclosure instead of duplicating them across the working page. Use readable labels/dates and consistently styled status badges.

Represent a customer once with related inquiries, versions and differences beneath it. Recommended matching: exact normalized email/provider identity proposes a match; ambiguous name matches require review. Do not silently merge customers or discard inquiry history.

Add a named Actions menu for edit/archive and relevant record actions. Default removal to reversible archive; show downstream consequences for linked contracts, bookings or invoices. Put the event calendar on the main Building page, starting Sunday and displaying local event times and booking names.

Fix contract creation and record loading using the actual failing record/response. Failed saves retain values and provide a next action. Mark truly required fields; relax requirements only where the downstream operation permits it.

After pricing changes, present current quote/invoice version and the valid next billing action. Draft invoices may follow the provider's supported update flow; finalized or paid invoices require the supported amendment/credit/reissue workflow. Preserve prior versions and provider identifiers. Repeated clicks must not create duplicate invoices.

Acceptance: workspace and event inquiries both appear; one customer can retain multiple inquiries; archive/restore preserves history; Sunday-first calendar agrees with source dates; draft-to-contract and price-change-to-invoice workflows pass with provider fakes and error/retry cases. A missing dependency is Blocked with explanation, not an inert button.

Affected area: building_page.py; Building CRM, calendar, contract and billing routers/services. Coordinate public inquiry fixes with B1.

### A5 — Finance selection and layout · P2

Sources: 5MJz9BrE91O_, sWf0sDlh8eoL.

Make the referenced four-metric group four columns on desktop, responsive below desktop. Do not let an old spacing comment replace the current Finance information architecture.

Allow multiple pending selections followed by one explicit Save. Show unsaved count, preserve other rows during save, support failure recovery and session undo using the shared queue contract. Save the batch atomically with one downstream refresh.

Acceptance: selecting three answers causes no page reload; one Save persists all three or explains failure without losing them; visible labels distinguish posted cash, confirmed/expected income, required payments and exceptions.

Coordinate with finance-ui-reset-spec.md; its proposed architecture is not automatically adopted by this comment delta. Owner: cashflow/finance_pages.py and existing queue commands.

### A6 — HR period selection and calendar readiness · P2

Sources: 4sKSOipGJ98l, FlGwDOQjVZ6H, rfGd_gRB2JEH, jpZu1cIGlCAJ.

Select from actual configured payroll periods, showing start/end and pay date. Do not substitute an arbitrary calendar day. Add edge fades only while a horizontally scrollable region has hidden content.

OOO setup must identify connected/not configured/permission failed, show the configured service-account email when available, and provide an operator-readable next step. Missing credentials belong to an administrator setup task; never display a made-up email or secret JSON.

Acceptance: period selection uses an existing period ID; each scroll edge indicator disappears at that edge; configured account email is visible to the authorized administrator; missing configuration and rejected calendar permission produce distinct actionable states.

Reconcile with hr-live-readiness-remediation-spec.md. Owner: hr/pages.py, hr_router.py and calendar adapter.

## 5. Anata website

### W1 — Correct Intelligence entry, shared pricing and compact visuals · P1/P2/P3

Sources: four open website threads. Related resolved history: Pu9VrEto1k-a, 8hQiTpqbfVsQ, qhX_gS08nKdM, dgwEWnuPeDpt, xMiIdhZxCpbO.

Route every Intelligence plan CTA to its actual paid onboarding flow. The historically documented destination is dashboard.anatainc.com/signup; implementation must verify the current plan-selection contract and preserve selected plan/account type. Shipping OS remains a separate destination. Audit the shared constant and all consuming CTAs, not only the selected button.

Use one approved Intelligence pricing data source/component on both the Intelligence product page and existing Pricing page. Duplicate only the Intelligence presentation, not unrelated service panels. Pricing is already in navigation; make the Intelligence entry discoverable there without adding a second Pricing destination. Show Brand/Portfolio distinction and included managed-client access accurately.

Do not revive earlier free-version or trial language: later resolved feedback explicitly removes it. Missing approved plan amounts are a publication dependency, not permission to invent prices.

Remove the selected “One system” center caption and blue backing while retaining the real icon and visible connectors. Reduce homepage signal-list gaps using the existing spacing scale, preserving horizontal desktop and vertical mobile compositions.

Acceptance: all Intelligence CTA destinations and plan context pass link tests; neither pricing surface diverges from the approved plan source; no obsolete trial copy; center icon remains legible; desktop/phone labels do not overlap. No newsletter work is added: hRIlzjxklDOd explicitly records its deferral.

## 6. Public Anata Building

### B1 — One reliable date-to-estimate-to-inquiry journey · P1/P2

Sources: 14 public form/booking/tour threads; coordinate A4. Form-page media requests share B2's asset and motion requirements.

Combine Arena estimating and date inquiry into one coherent flow. Carry selections forward instead of asking attendance twice. Support a start/end date range, local start/end time, and named half-day choices defined by the approved rate plan. Replace “booking units” with the actual customer choice; do not prefill an unexplained quantity of one.

Show estimated pricing after enough valid inputs exist, with included items and exclusions. Use the effective approved rate version. No approved plan means request-a-quote with an honest explanation; it is not a zero-price estimate or a confirmed reservation.

Align client and server validation. Preserve completed fields after failures; show field-specific correction and an error summary. Successful persistence returns a stable inquiry reference. Retry must not duplicate the lead. Notification delivery is tracked separately from successful capture, and operators can see the destination and delivery state.

Acceptance: same-day, multiday and approved half-day scenarios yield reproducible totals; invalid ranges fail specifically; duplicate attendance is removed; retry after provider failure preserves one inquiry; that inquiry appears in A4 with traceable notification routing. Tour acknowledgment reflects the configured operational process.

### B2 — Approved pricing and authentic public content · P2/P3

Sources: remaining nine Building threads, plus the media requirements on B1 form pages.

Publish coworking plans and Spaces pricing only from approved current inventory/rate plans. The Havn Sanctuary mention is a design-reference request; inspect the exact reference before visual implementation, and never borrow its prices as Anata prices.

Remove the blanket “Bullpen is full” marketing status from the selected area. Use availability-safe inquiry copy; retain accurate availability handling in the booking flow.

Use approved full-facility, lifestyle and gym media; reuse homepage video on Events where appropriate. Provide a documented upload/placement route. Add tenant logos and human review presentation using real approved assets. Replace the selected em dash in homepage copy. Preserve the “Coming soon” request on its actual selected target, as recorded in the ledger.

Acceptance: draft pricing never reaches public output; updates use the same approved version on relevant pages; no unapproved testimonial/logo; media has alt text, appropriate crop and motion controls; inquiry forms remain usable without animation. Missing media is an asset dependency with explicit placement.

## 7. Onyx Bio

These are product requirements based on comments, not a legal, clinical or tax review. Inspect Onyx's own rules and current code before implementation.

### O1 — Familiar staff workspace · P2

Use searchable, sortable, paginated tables for orders, returns, operations and account lists, with canonical detail links and in-place editing where appropriate. Show names and emails in place of bare user IDs. Explain permissions in task language.

Order details show line items, quantities, prices, promotions and separate internal/customer-facing notes. Scope the first release to supported draft edits; paid/fulfilled adjustments use existing authorized adjustment flows and audit, not silent total mutation. Link refund/return requests to the customer's order and make them searchable by order number/customer.

Consolidate CRM stage definitions and add-stage into one table/dialog; reorder with drag plus keyboard controls. Keep configuration separate from daily sales records. Provide copyable absolute brand-intake link, sortable catalogue headers and an image-update path. Show account incompleteness by account type/program, with exact corrective links.

Acceptance: staff can locate an order and its account, inspect items, save a supported edit and reload it; unauthorized edits fail server-side; account requirements vary correctly; stage order persists without orphaning records.

### O2 — Shipping and payment readiness · P1/P2

Consolidate duplicate shipping settings. First verify Elite Works rating support against the actual integration contract; do not assume availability from the provider name. A quote must include destination/package inputs, carrier/service, amount/currency and expiry. Apply an explicit configured handling margin and rounding rule; store provider amount, adjustment and customer total separately.

Recommended rounding is upward to the next $0.10 only after the owner confirms that “tenths” means ten cents; margin amount remains a required decision. Requote when address/package changes and revalidate before purchase.

Payment readiness reflects actual provider setup. The checkout thread reports that a Fiserv adapter exists and production credentials/merchant approval remain; verify this claim rather than redesigning checkout or claiming it is live.

Acceptance: expired quotes cannot be purchased silently; address changes invalidate rates; payment-unavailable state does not accept a charge; provider retries cannot create duplicate orders/payments. No live purchase is part of this review.

### O3 — Organized marketing and real campaign data · P2

Separate campaigns, transactional templates, audiences, seminars and site banners under one Marketing navigation group. Scheduling has one canonical editor.

Use persisted audience filters and preview counts, dynamic personalization with safe fallback values, and template previews. Provide current/past/inactive promotion history and an honest empty state.

Create scheduled banner records with copy, CTA, audience, start/end, priority and enabled state. Recommended default: show the single highest-priority eligible banner with deterministic tie-breaking, rather than rotation. Require explicit approval of first-order discount amount, eligibility, expiry and stacking before activation.

Quality incident communication is a draft based on linked affected order/item/lot evidence, not an automatic blast. External audience sync and sends require their own authorized workflow.

Acceptance: preview matches recipient data and fallback; consent/suppression is enforced by the existing system; banner start/end boundaries are deterministic; promotion history is complete; drafting a quality notice sends nothing.

### O4 — Quality, tax documents and publication dependencies · P1/P2

Link quality records to affected lots/items/orders and evidence. Add authenticated contact/support entry, including the requested contact-professional path. Preserve the user's account requirement and return to the intended form after sign-in.

Expose tax-document pending/validated/expired states, validity dates and audit. Whether tax is charged for a particular transaction must follow reviewed jurisdiction/product/customer rules; a missing upload alone cannot define a universal tax policy.

Terms/privacy drafts require the designated review and approved versions before replacing draft notices. The final contact/sender address depends on the approved domain and verified mail configuration.

Acceptance: role checks protect quality/admin data; contact requests preserve account linkage; expired exemptions follow the configured reviewed policy; unreviewed legal text is not presented as approved.

### O5 — Customer journey and visual consistency · P2/P3

Show a cart entry with quantity reflecting persisted contents. Clarify “apply” for programs, account setup and missing requirements. Permit authorized admin viewing of product information without granting unintended customer purchase rights.

Align typography, card treatments, active navigation and spacing with the owning brand. Replace requested placeholders with approved catalogue/lifestyle assets. Seminars may use “Coming soon” without inventing dates or availability.

Acceptance: cart count survives navigation/reload; account setup links reach the missing fields; admin view and buyer eligibility are independently tested; no unsupported product claims are introduced by new copy or imagery.

## 8. Ascend

### S1 — Readable, growth-partner positioning · P2/P3

Sources: all five Ascend comments.

Fix contrast on the homepage ticker and sell-your-brand form. Replace selected capital/investment-heavy copy with concrete growth-operator partnership language. Proposed heading: “Partner with operators who help brands grow.” Keep actual business terms factual.

Use approved founding-team portraits. Treat the possible color swap as a decision: default to existing brand colors with corrected accessible contrast.

Acceptance: text contrast reaches WCAG AA (4.5:1 normal, 3:1 large); keyboard/focus states remain visible; portraits match named team members; the selected section leads with operating expertise without inventing performance promises.

## 9. Shared states, data and API requirements

Every affected workflow needs loading/saving, success, empty, validation error, permission denial, stale/conflict and provider-unavailable behavior where applicable. Keep entered values on failure; disable duplicate submissions while pending; distinguish saved locally from synchronized/delivered externally.

Use stable IDs, server-side capability checks and audit for writes. New lists apply filtering/sorting before pagination. Money uses explicit currency and deterministic decimal arithmetic. Estimates include assumptions and source version. Schedule/date operations use named timezones.

Prefer additive migrations: new record links, versions, configuration and audit fields only where missing. Backfill customer relationships without deleting source rows. Do not migrate tables for a purely visual change.

No new analytics platform is required. Use existing logs/events for job due-to-start delay, inquiry persistence/delivery outcome, deck readiness failures, provider errors and changed-record audit. Avoid logging secrets or recipient token URLs.

## 10. Validation and rollout

1. Map each ledger ID to current code and the actual deployed revision. Classify as still reproducible, already implemented, superseded, or decision-blocked. Record evidence; do not claim closure from code alone.
2. Deliver P1 slices independently by repository: Intelligence routing; deck evidence; missed outbound jobs; Building submission/contract/billing recovery. Then operational tables/setup, then visuals.
3. Test deterministic arithmetic, date/DST handling, persistence, concurrency/idempotency and server authorization with sanitized fixtures/provider fakes.
4. Browser-verify complete affected journeys in preview, including failure and reload. At minimum use 390, 768, 1280 and 1440px, keyboard, focus, reduced motion and 200% zoom; verify deck print/export separately.
5. During later implementation, follow each repository's deployment rules. Agent features/fixes are deploy-by-default after validation. Verify the exact production revision and keep unrelated changes out.
6. Roll back UI/application code to the prior known-good deployment when needed; retain additive data and audit. Never “undo” a provider payment by deleting a local record.
7. Resolve comments only in a separately authorized implementation/review workflow with deployment and acceptance evidence. This specification leaves all Vercel comments unchanged.

Definition of done for a slice: every mapped open thread has a specific disposition, implemented requirements pass their criteria, blocked decisions are explicit, and evidence identifies the tested deployment. The complete ledger is the coverage checklist.

## 11. Decisions and recommended defaults

| Decision | Recommended default / dependency |
| --- | --- |
| Intelligence plan destination and amounts | Verify paid onboarding contract; reuse approved plans; never Shipping OS |
| Pricing route duplication | Extend existing /pricing and reuse only the Intelligence panel |
| Building half-day rules and plan publication | Use approved effective rate plans; no invented hours/prices |
| Building duplicate customers | Propose exact-identity links; review ambiguous matches; preserve history |
| Archive versus delete | Reversible archive by default for linked operational records |
| Onyx shipping margin/rounding | Owner must specify margin; propose next $0.10 after clarification |
| Onyx first-order discount | Leave inactive until amount, eligibility, expiry and stacking are specified |
| Onyx banners | One highest-priority eligible campaign |
| Onyx legal/tax policy | Designated review; explicit approved versions/rules |
| Missing portraits, gym imagery, tenant logos | Approved asset owner and upload location before publication |
| Ascend palette | Retain current palette and fix contrast |
| Historical newsletter request | Keep deferred per its recorded decision |

## 12. Deliverables and next phase

This review adds only this spec and the complete comment ledger. Existing specs and application files remain as found.

Recommended first implementation batch: A1 People search, W1 Intelligence destination correction, and reproduction/triage of A2/A3/A4 P1 failures. These are separate deployable slices, not a combined cross-repository release. Broader Onyx commerce/campaign changes follow their listed provider and business decisions.
