# Fulfillment proposal pricing and publishing flow

Status: Approved for implementation; shipping and production validation in progress.
Scope: Standalone fulfillment sales review and customer proposal, with an optional HubSpot handoff.

## Problem and evidence

Sales needs to turn a shipping-rate inquiry into a reviewed fulfillment proposal, publish the agreed customer prices, and share a complete proposal without involving HubSpot.

Run 3716 (Bloom Towels) demonstrates the failure. During the September 11 review, the admin showed saved pick-and-pack pricing of $2.10/order and a customer monthly estimate of approximately $9,945. The embedded proposal displayed carrier postage only and explicitly excluded fulfillment fees. The admin nevertheless said the fees would appear publicly and that quote readiness checks passed.

Verified implementation:

- `fulfillment_public_router.py` creates public teasers with `suppress_fulfillment_pricing=True`.
- `fulfillment_deck/service.py::rerender_rate_sheet` preserves that flag after sales edits.
- `fulfillment_deck/rendering.py::render_rate_sheet_html` omits Full rate card and Estimated invoice when the flag is set.
- The update route saves fee overrides but has no proposal conversion action.
- Admin preview and the hosted public page read the same `summary_json.deck_html`. Saving a published run therefore changes public content immediately.
- `storage.publish_run` updates publication status/time; it does not establish an independent content snapshot. The publish route can also initiate HubSpot quote sync.
- Quote readiness checks do not require a fulfillment proposal with visible pricing or associate review approval with a particular pricing revision.

Inference: run 3716 likely inherited its suppression flag from the public funnel. Its original creation source was not independently read from production storage.

## Users, constraints, and scope

Users: Sales owns final customer pricing and publication; fulfillment owns signed warehouse cost inputs; prospects read published customer proposals.

Preserve deterministic pricing rules, agreement defaults, signed cost collection, existing permissions, customer URLs/tokens, engagement tracking, and lead/deal associations. Internal costs, margins, waiver justifications, and operator metadata remain private. Follow DESIGN.md and the shared Agent shell; introduce no UI framework.

This change covers proposal mode, pricing review, draft/public separation, and quote consistency. It does not redesign the carrier map, change pricing formulas or approval thresholds, rebuild the CRM, or send customer communications. Shipping OS remains a separate service segment and must not acquire a 3PL invoice through this fix.

## Target workflow

1. **Shipping teaser.** Public generation still produces a postage-only asset. The sales workbench explicitly labels it “Shipping teaser — fulfillment pricing is not included.” Internal cost collection can proceed independently.
2. **Prepare fulfillment proposal.** A prominent action converts the working draft to a fulfillment proposal. Reuse the same run, prospect, products, deal, and saved rates. Do not create a second lead or require re-entry. Conversion immediately enables the customer rate card and estimated invoice in the draft preview; missing inputs are labeled and block publication, not drafting. For a Shipping OS record, require an explicit service-segment change before conversion.
3. **Set customer pricing.** Present final customer prices beside warehouse costs in the existing pricing workspace, with defaults and waived lines explicit. Show the customer estimate separately from internal contribution/margin figures. Customer rendering, admin customer estimate, and quote lines consume one resolved customer-pricing result, including all overrides and waivers.
4. **Save draft.** Save inputs and rebuild the admin preview. Show “Draft saved. Live proposal unchanged.” Saving has no HubSpot quote or customer-email side effects. If rendering fails, preserve saved inputs, mark the preview Stale, and block publication until a successful rebuild.
5. **Review pricing.** Sales approves the successfully rendered revision, with author and timestamp. Changing price, waiver, volume, package, shipping assumptions, service segment, or costs that affect approval invalidates that review. Deal changes invalidate quote readiness. Approval can accompany Save if the server binds it to the successfully computed revision; a sticky checkbox from an older revision is insufficient.
6. **Publish proposal / Update live proposal.** Validate the current draft and atomically publish its customer-safe snapshot. Keep the existing share URL. Publish does not regenerate carrier rates or create a quote. The operator publishes exactly the preview they reviewed. Show publication time and revision, with a distinct “Unpublished changes” indicator on later edits.
7. **Share proposal.** Publishing completes the core workflow. Show “Ready to share” with Copy link, Open live proposal, and Print / Save PDF. No CRM setup, deal, quote, or sync is required. This proposal is not a new native contract-signing or payment system.

**Optional: Create HubSpot quote.** A secondary integration action, used only when Sales chooses it, creates the quote from the published proposal revision. Show the quote link only after verified success. A failure leaves the published proposal available and offers retry. If a later revision is published, label the previous quote Stale and require explicit quote update/recreation under existing HubSpot capabilities; do not silently modify an accepted quote.

Do not make “Sales pricing reviewed” secretly change the document type. Conversion is visible and purposeful; review records approval of the resulting proposal.

## Review page and readiness

Use one compact workflow summary: document type, draft/public state, cost readiness, and pricing review. Choose one primary next action in this order: prepare proposal; complete required inputs; review pricing; publish changes; share proposal. Put HubSpot deal and quote status in an optional integration disclosure. An absent deal, uncreated quote, or integration failure must not mark the proposal incomplete or replace Share proposal as its next action.

Place customer pricing and readiness ahead of the long preview. Label the preview “Draft customer preview”; offer “View live proposal” separately. Consolidate overlapping save buttons into “Save draft” with one shared form behavior. Remove contradictory copy that suggests Save both does and does not update the public URL. Hide inapplicable 3PL editing for Shipping OS or explain the required segment change.

Proposal publication requires valid resolved customer prices, successfully rendered current content, a signed cost submission applicable to the current cost inputs, current sales review, waiver reasons, and any existing margin approval. Missing or invalid values must produce specific blockers rather than silently passing. Preserve configured business thresholds. HubSpot deal association is required to create the quote; it is not needed merely to draft or publish a customer proposal. Teaser publication retains its existing postage-only requirements.

Empty inputs link to the relevant field or cost form. Running operations disable only the submitted action and show progress. Failed saves preserve entered values and surface field errors. Stale revision submissions return a conflict and ask the operator to reload/reconcile; they never overwrite a newer draft. Unauthorized mutations retain existing authentication/authorization enforcement. Use keyboard-accessible native controls, associated error messages, announced success/failure, and contained table overflow at mobile widths.

### HubSpot independence — required

No HubSpot credentials, connection, deal ID, healthy API, or quote may be required to load the workbench, collect costs, price, review, preview, publish, or share. Separate proposal-readiness validation from optional HubSpot-quote readiness; do not reuse CRM-required guards for proposal publication. Unavailable HubSpot shows a non-blocking message only in its integration section. Do not queue automatic CRM retries from core proposal actions. Existing associations may be retained without contacting HubSpot.

## Data and API contract

Use the existing run and summary storage for an incremental implementation, with a versioned envelope rather than a new application framework:

- `document_kind`: `shipping_teaser` or `fulfillment_proposal`; independent of existing `segment` (`dfy`/`diy`). Derive the legacy suppression flag from document kind for compatibility; it ceases to be an independently writable source of truth.
- `draft_revision`: monotonic version of proposal-affecting inputs. Store the successfully rendered revision separately so stale HTML cannot be published.
- `pricing_review`: reviewed revision, actor, time, and relevant approval references. Track the applicable cost-input revision with fulfillment signoff; historical signatures alone must not approve changed costs.
- `published_snapshot`: revision, kind, publication metadata, rendered customer HTML, allowlisted customer pricing/estimate, and the carrier/profile inputs needed for customer interactions. Never copy the entire internal summary into a public response.
- Quote sync records: published revision, stable idempotency key, external quote identifier, status, and error summary. Resolve private deal metadata on the server; expose no costs or internal approvals in customer quote payloads.

Keep current update, preview, publish, and quote route paths. Add an authenticated conversion action. Mutation forms submit their expected draft revision; check/update under a database lock or equivalent compare-and-swap transaction. Save returns to the workbench with draft status. Publish copies the already-rendered revision atomically after validation; do not hold a database transaction open across WMS or HubSpot calls.

Public HTML, exports/print, and public re-quote inputs must use the published snapshot. Public re-quotes remain non-persistent and cannot mutate canonical draft or published content. Admin re-quotes alter only the draft and invalidate review where applicable. Audit all code paths that read mutable summary fields, including first-view metadata and quote building, so a published page cannot mix revisions.

Keep the marketing public matrix serializer postage-only and allowlisted. Do not expose fulfillment pricing by broadening its existing payload. The authenticated preview and token-gated customer proposal use the proposal renderer; the teaser marketing experience stays scoped to carrier rates.

## Migration and Bloom Towels recovery

1. Backfill document kind from the existing suppression flag. Preserve existing segment. Do not infer conversion from prices or a reviewed checkbox.
2. For already-published runs, snapshot the currently served content before allowing any new draft write. This preserves existing live behavior; historical pre-save content cannot be reconstructed unless separately retained. Do not change tokens or URLs.
3. Retain legacy fields as compatibility mirrors during rollout, but switch public readers to snapshots and prevent fallback to mutable draft HTML once migrated. Migration is idempotent and does not create quotes or change fees.
4. List teaser runs with saved fulfillment pricing as “Needs review — prepare fulfillment proposal.” Do not bulk expose their prices.
5. For run 3716, verify its current data again, preserve all overrides/waivers, convert its draft, and confirm the $2.10 pick fee and the recomputed monthly total in the preview. The earlier $9,945 is evidence from review, not a hardcoded expected total. Review and publish the corrected proposal at the existing URL. Recovery is complete when the corrected proposal is published and shareable. Leave HubSpot untouched unless Sales separately chooses the optional quote action.

## Implementation targets and sequence

1. `storage.py`: versioned draft/published state, migration, atomic publication, conflict handling, history records. Add snapshot readers before changing Save behavior.
2. `service.py`, `pricing_rules.py`, `quote.py`, and `rendering.py`: explicit document kind, shared resolved pricing, revision-bound readiness, render without fresh rates at publish time.
3. `fulfillment_deck_router.py`, `admin_page.py`: conversion action, accurate controls/copy, draft-only save, separate quote action, preview/live distinction.
4. Remove implicit HubSpot calls from the core review, conversion, save, and publish paths, including margin sync, deal linking, and dropdown loading as blocking dependencies. Render optional CRM context from cached/local data or a separately loaded panel. If the existing quote action cannot safely consume a published revision, temporarily disable only that action with an explanation. Improvements in `hubspot_sync.py` can ship separately; they must not delay the standalone proposal fix.
5. Public routes and consumers: migrate snapshot reads, preserve teaser serialization and public re-quote isolation. Document operator behavior in the fulfillment setup documentation.
6. Validate in staging, deploy the compatible changes, then perform the targeted run 3716 repair and production verification. Keep unrelated workspace edits out of the eventual deploy commit. This specification alone authorizes no production data repair.

## Acceptance criteria and validation

- A teaser with stored fee overrides visibly explains why it contains postage only and offers conversion.
- Converting a DFY teaser reveals the full customer fee card and invoice in draft preview without changing its live content, URL, run, or deal.
- A fixture with $2.10 pick-and-pack, waived setup, and additional-item/receiving overrides produces matching customer fee values and applicable totals across admin estimate, preview, published proposal, and quote. Show one-time/conditional fees separately from recurring totals.
- Customer HTML, payloads, exports, and quote lines contain no warehouse costs, margin figures, or private waiver reasons.
- Saving a published proposal changes only the draft. Reading the public URL and public re-quote baseline before and after Save returns the same published version.
- Publishing atomically switches the reviewed version without a new WMS fetch or HubSpot write. Failed rendering, failed validation, or a stale revision leaves the prior public version intact.
- Any relevant edit invalidates pricing approval. New costs do not inherit approval solely because an old signed submission exists.
- With HubSpot unconfigured, disconnected, or failing, a user can complete the entire proposal workflow and share the live link/PDF without delay or CRM-related blockers. Core actions perform no HubSpot calls.
- Optional quote criteria apply only when the integration action is enabled; its repair is not required to release the standalone fix.
- A quote cannot be created from a teaser or unpublished working prices. Duplicate requests/retries for the same revision do not create duplicate quotes. Publishing newer pricing marks the old quote Stale.
- DIY/Shipping OS output and marketing teaser responses remain postage-only. Existing tokens, tracking, print, and public recovery behavior still work.
- Migration is repeatable, preserves existing published output, and does not auto-convert any prospect.

Use focused service/storage/route tests for these contracts, plus one browser journey from teaser through conversion, costs, pricing review, Save, Publish, and sharing with HubSpot disconnected and no deal ID. Repeat with HubSpot requests forced to fail/time out; core actions must not invoke or wait for them. Test optional quote creation separately against a sandbox CRM if that action remains enabled. Verify a second browser session sees only published revisions. Exercise a render failure, stale concurrent edit, and, separately, optional quote retry. Check keyboard operation and desktop/mobile layouts. In production, verify run 3716's published customer prices and privacy boundaries without sending an email.

## Recommended decisions

Adopt explicit one-time conversion, separate Save and Publish, and a complete standalone sharing flow as one cohesive fix. HubSpot is optional and its repair is not a release gate. A flag-only patch would restore this deck but leave the misleading workflow and accidental live updates intact. No clarification is needed to implement this proposed default once the specification is approved.

Before enabling the optional quote action, confirm the existing HubSpot integration's support for quote updates and its accepted-quote detection; default to explicit creation of a replacement draft when safe updating is unsupported. Record conversion, save, review, publication, conflict, and quote outcomes in existing activity history with actor and revision; no new analytics platform is needed.
