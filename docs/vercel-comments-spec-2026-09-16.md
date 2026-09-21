# Vercel feedback implementation spec — 2026-09-16

Status: Proposed. Specification only.

Design follow-up: read the [live UI audit and page compositions](vercel-comments-ui-design-audit-2026-09-16.md) before implementation. It refines this spec and corrects the local-source baseline: draft/public proposal controls and several Intelligence changes are already visible in production, and the live pipeline presents manual refresh. Reproduce remaining behavior on the deployed revision before implementing older findings.

## Scope and evidence

Review covers the anata Vercel team across all projects, branches, pages and both comment statuses: **167 open, 233 resolved, 400 total**. The [coverage ledger](vercel-comments-review-ledger-2026-09-16.md) links every thread. Compared with September 10, there are 16 additional open threads: 13 website and three Agent. The earlier 384 IDs remain present. Carry forward the [September 10 requirements](vercel-comments-consolidated-spec-2026-09-10.md) for the 151 older open threads; this document adds the eight slices below. Resolved history remains a regression reference, not a completion certificate.

Users: prospective Anata customers comparing services and software; sales operators preparing fulfillment proposals and optionally associating CRM records.

Outcome: readable, coherent website presentation and a fulfillment workflow that preserves operator work, exposes relevant pricing controls, and permits company/contact creation without leaving a deal draft.

Verified local evidence:
- Agent `sales_support_agent/services/fulfillment_deck/admin_page.py:1636` emits a full-page reload after 8 seconds when `has_running` is true. This supports the interruption report; the affected live revision was not reproduced.
- `sales_support_agent/api/sales_router.py` renders company/contact selectors and matches existing options from contextual name, domain and email. The inspected form has no inline creation controls.
- `sales_support_agent/integrations/hubspot.py` already exposes `create_company` and `create_contact`; use this integration boundary.
- The review renderer already has fee waiver controls and final price overrides. “Off” must not acquire a new, ambiguous financial meaning.
- The sibling checkout `../anata Website/src/components/home/v6/HeroV6.tsx:86` uses `clamp(1.9rem, 5.2vw, 4.6rem)`. Its pricing route still uses UtilityPage, while newer comment DOM context shows a different pricing implementation. The checkout is therefore a source reference, not the authoritative deployed baseline.

Before website implementation, identify the deployment associated with the comments and start from its current source revision. Read that repository's own AGENTS.md and design authority. Do not overwrite newer pricing or transition work with the older sibling checkout. Comment screenshots were not visually inspected; selector and selected text identify targets, but are not substitutes for browser verification.

## Constraints and non-goals

Follow Agent AGENTS.md and DESIGN.md for operator pages, and the owning website's brand rules for public pages. Preserve endpoint contracts, pricing formulas, source data, permissions, public proposal tokens and audit. No new UI framework, invented assets, invented performance claims, automatic customer communications, or blanket redesign.

This is not authorization to modify CRM records, publish proposals, change commercial terms or resolve Vercel comments during this review. Implementation is a later task. Existing unrelated workspace edits remain outside this deliverable.

Coordinate A9 with [fulfillment proposal pricing flow](fulfillment-proposal-pricing-flow-spec.md): standalone proposal preparation, publication and sharing must remain independent of HubSpot. That proposed draft/public separation is a dependency if this slice promises draft-only saving; do not claim it already exists.

## A7 — Preserve work while fulfillment jobs run · P1

Source: ESfSQHTwQ3dL, /admin/fulfillment/sales.

Replace timed document reload with targeted updates of run status and completed results. Preserve filters, pagination, expanded records, scroll, focus and unsaved input. Default polling cadence: 8 seconds while visible and jobs are running; one request at a time. Pause when hidden, refresh on return, stop at terminal states. A failed request shows “Updates paused” with Retry and the last successful update time; retain existing rows and back off rather than navigate or empty the workspace.

Do not replace fields being edited when results arrive. If a changed record cannot be reconciled safely, show “New results available” with an explicit update action. A job failure appears on its row with a useful next action. Empty means no matching runs, not a fetch failure. Without JavaScript, provide a normal manual refresh link.

Implementation: admin_page.py and fulfillment_deck_router.py. Reuse a suitable authenticated status reader if available; otherwise add a read-only endpoint under the existing fulfillment sales namespace returning stable run ID, status, update timestamp and allowlisted row display data. Scope records to existing permissions. Do not return internal payloads unnecessarily or trigger jobs from polling.

Acceptance:
- With a running job, type an unsaved note, expand a record, filter and scroll; after at least three polling cycles every state remains intact and no document navigation occurs.
- Completion updates the correct row without duplicating it or touching another record's input.
- Failed polling, session expiry and job failure have distinct recoverable states. Unsaved text survives the failure.
- The notes save path checks response success before showing Saved; failed saves retain the value and offer retry.

## A8 — Create company and contact from a deal draft · P1

Source: qMFSxox4vyY_, /admin/sales/deals/create, reached from fulfillment review run 3716.

Add “New company” and “New contact” beside their selectors. Open compact accessible dialogs prefilled with available prospect context. Suggested required fields: company name; contact email. Domain and contact names are optional unless current configured CRM rules require them. Make actual requirements explicit.

Workflow: enter details; validate; show potential existing matches; choose an existing record or explicitly create; save through the existing HubSpot service; select the returned stable record ID in the original draft. Preserve every deal field, service line, rate-sheet run ID and validated local return destination. Cancel restores focus and does not create anything. Creation must not submit the deal automatically.

Normalize domains and email for duplicate lookup. Never merge on a fuzzy name match. Use an operation identifier and persisted creation outcome so retry after an uncertain timeout can reconcile the prior result rather than blindly create another record. If company creation succeeds and contact creation fails, retain the company selection and retry only the contact operation. Validate record access again at deal submission; do not assume the initial dropdown is authoritative.

Server-side capability checks, existing request protections, sanitized errors and actor/object/outcome audit apply to both actions. A read-only or disconnected integration explains the unavailable action without disabling standalone proposal work.

Acceptance: create and select each entity while retaining the draft; choose an existing match; cancel with no write; double-click/timeout retry without duplicate creation; recover from partial success; deny unauthorized writes. Use provider fakes for write verification.

Targets: sales_router.py, integrations/hubspot.py and existing sales rules/association services. Add small authenticated creation handlers and a persisted operation result only if existing write infrastructure cannot supply it. Do not build a parallel CRM.

## A9 — Compact, explicit pricing review · P2

Source: dk-YdTpzBtYh, /admin/fulfillment/sales/runs/3716/review.

Keep line name, internal cost, suggested price and editable final customer price visible. Move explanatory prose into a keyboard- and touch-accessible information disclosure with a meaningful accessible name. Required units, validation and financial consequences remain visible.

Default interpretation of “toggle options”: optional charge inclusion, not arbitrary hiding of required fees. Use “Include charge” for eligible optional lines. Turning it off retains its previous entered price, excludes it from the customer total and labels it Not included. Turning it on restores the saved price. Required lines stay enabled; existing fee waiver and margin approval policies remain explicit. Do not equate an excluded charge, a waived charge and a zero-price included charge. Do not suppress an underlying required service or cost by hiding its row.

Save through the existing deterministic pricing resolver. Admin estimate, customer rendering and optional quote lines must agree on included charges. If the current waiver model cannot express exclusion distinctly, add a stable fee-keyed inclusion field; legacy records default to their existing behavior. Audit changes and invalidate pricing review where pricing changes. Preserve the standalone proposal spec's draft/public boundary when implemented.

Acceptance: help works on focus, click and touch; a line can be disabled and restored without losing its value; required charges cannot bypass policy; reload reproduces saved inclusion and totals; errors retain edits; customer output contains no internal cost or margin data. Validate included, excluded, waived and zero-valued cases with meaningful pricing fixtures.

## W2 — Restore homepage headline hierarchy · P2

Sources: NFaOWiyPZvnD and 7-OLYhwYGwsH, /.

Remove the specifically rejected supporting sentence beneath the hero (“Amazon marketing, ecommerce fulfillment and shipping. One team and the software behind it.”) and its enclosing emphasis treatment if present. Do not introduce another banner or card to compensate. Preserve required semantic heading and service navigation.

Increase the prominence of “Sell more. Ship smarter. Know why.” on mobile and tablet. The latest comment also requests desktop emphasis and supersedes the earlier “desktop looks great” note where they conflict. Tune size against available width; do not force overflow through nowrap. Maintain the intended three-line composition where it fits, and permit accessible reflow at zoom.

Acceptance: before/after captures at 390, 631, 799, 1280 and 1440px show a visibly more prominent headline, no rejected paragraph/panel, and no clipping, overlap or hidden CTA. At 200% zoom the text remains readable and navigation usable.

Likely source: HeroV6.tsx and owning hero styles; confirm against deployed source.

## W3 — Plain-language positioning and stronger proof · P2

Sources: qG9eEtwTOA81 and v_aSvQgK1Vkz, /.

Replace the selected technical paragraph with concrete choice language. Proposed copy: “Use our software with your team, work alongside our specialists, or let us manage the work for you.” Align the wording with actual available service models.

Rework Proof to show client, verified product category, main result, measurement period and linked case study in one readable composition. Give the main result visual emphasis; separate supporting measures. Expand context from approved case-study material rather than adding unsupported results. Existing selected text mentions Sonrei, Ocean Rx and Zantrex; these are candidates to reconcile against source, not permission to invent new category names, results or dates.

Acceptance: each published claim and category has an approved source; time period accompanies the metric; links reach the matching case study; no empty category badges or repeated decorative statistics. When additional approved evidence is unavailable, retain the verified story and identify the missing content in implementation notes.

Targets: homepage proof and positioning sections; locate actual consumers, with ProofStories.tsx a local candidate.

## W4 — Pricing width, imagery and icon movement · P2/P3

Sources: Az-P2j9sXEzo, KbRUz4QG9dOp and Nwg8BTyM2Vex, /pricing.

Expand the selected pricing information section to the established page container width. Keep paragraph line lengths readable inside it. Add relevant existing Anata icons and reuse an approved lifestyle image from another site page, with deliberate crop, responsive sizes, reserved dimensions and appropriate alt text.

Make the existing decorative icon cycle modestly faster. Proposed starting point: reduce duration by 20%, retaining amplitude and easing; visually tune from the current deployed implementation. Reduced motion uses a static composition. Do not add animation dependencies.

Acceptance: the selected section aligns with adjacent content; icons describe their groups; lifestyle imagery is visible without displacing the primary pricing decision; no layout shift from unloaded imagery; animation does not obstruct text or controls. Preserve existing approved plans, prices, product-specific CTAs and Brand/Portfolio data. Reuse the same Intelligence plan source on both pricing surfaces.

## W5 — Intelligence navigation and visual consistency · P2

Sources: YwRhko-yU5lv, EKVbBYZ5uOV-, Z1zDWk78vKSN, fexMSsMSLP8y and z3SbW0b--7c5.

Use one-word navigation labels: Optimization, Health, Strategy for the corresponding existing entries. Preserve route/anchor targets and accessible full context. Keep every item reachable at narrow widths.

Apply the established multicolor text treatment to “Every growth decision.” and “together,” preserving accessible contrast and plain-color fallback. Remove the selected visual's background square so its movement floats freely. Normalize its spacing and reserve its intrinsic area so removal does not cause overlap or layout shifts. The older standalone-icon request remains relevant.

Add a brief transition between Brand and Portfolio pricing panels; proposed duration 180ms. Maintain tab semantics, keyboard selection, selected state and sensible focus. Outgoing invisible controls cannot receive focus; rapid switching always ends on the last selected category. Respect reduced motion and avoid animated price counters.

Acceptance: requested labels do not wrap at normal tested widths; destinations are unchanged; both named text spans use existing brand treatment; the removed square leaves no unexplained blank region; tab changes preserve correct plans and CTAs and remain usable without animation.

Likely owners: IntelligenceHub.tsx, IntelligenceVisuals.tsx and the actual shared pricing selector.

## W6 — Consistent public-page transitions · P3

Source: d3lqHpdyTVkQ, reported on the Advertising Engine route with a request covering all public pages.

Apply a restrained shared route/section entry treatment, beginning with the existing transition implementation if present. Default 180–220ms opacity and at most 6px translation. Content must render immediately; no artificial loading delay, blocking overlay, or hidden content awaiting an observer. Avoid double animations from nested page and section wrappers.

Direct loads, internal navigation, browser back/forward and anchor links retain expected scroll and focus. Reduced motion removes movement. Loading, error and empty pages remain immediately legible; do not animate operator queues under this public-site request.

Acceptance: verify home, pricing, Intelligence, Advertising Engine and one unrelated public route with fast and slow loading, JavaScript unavailable, reduced motion and keyboard navigation. No blank frames, trapped focus or material layout shift.

## Backlog, validation and rollout

| Project | Open | Resolved | Existing requirement coverage |
| --- | ---: | ---: | --- |
| anata-website | 17 | 209 | W1 plus W2–W6 above |
| anata-agent-staging | 69 | 0 | A1–A6 plus A7–A9 above |
| anata-building | 23 | 0 | B1–B2 in September 10 spec |
| onyx-bio | 53 | 24 | O1–O5 in September 10 spec |
| ascend-companies | 5 | 0 | S1 in September 10 spec |

Implement independent slices: A7 first, then A8 and A9; website W2 first, then W3–W5, then W6. Older P1 data integrity, submission and scheduling failures retain their priority; a new cosmetic comment does not demote them.

Before each slice, record source thread IDs, actual deployment revision, current reproduction and disposition: still present, already addressed, superseded or decision-dependent. Do not reimplement solely because a Vercel thread remains open.

Validate affected workflows and boundary conditions, then browser-check at 390, 768, 1280 and 1440px plus the reported 631/799px cases. Include keyboard, visible focus, 200% zoom, reduced motion, request failure and persistence after reload. Use current approved pricing fixtures; never hardcode historical run 3716 totals.

No new analytics platform is needed. Reuse existing audit/events for creation outcomes, pricing changes and refresh failures; do not log full contact lists or token URLs. Visual slices require no database migration. A8 operation records and A9 inclusion state require additive, backward-compatible storage only if existing structures cannot represent them.

Later implementation follows each repository's deployment policy with isolated commits and exact production revision verification. Roll back application changes without deleting created CRM records or audit history. Comment closure requires evidence that mapped acceptance criteria pass.

## Decisions and defaults

- Headline conflict: latest feedback controls; improve desktop prominence as well as mobile/tablet.
- Pricing toggles: default to optional charge inclusion with explicit waived/excluded distinctions. Confirm the eligible fee list from existing rules before implementing toggles; no new business policy is implied.
- Proof expansion: approved categories, periods and result sources are a content dependency. Do not invent them.
- Motion speed: 20% faster icons and approximately 180ms transitions are proposed tuning defaults, not user-specified values.
- Website source mismatch: identify the actual deployed branch/revision before estimating file-level changes.
- HubSpot creation is optional integration work; it cannot become a requirement for publishing a standalone fulfillment proposal.

Deliverables from this review: this spec and its coverage ledger. No application implementation or deployment was performed.
