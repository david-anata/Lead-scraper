# Standard offers: DFY and DYI

Status: Ready for implementation with the defaults below. Specification only.

## Outcome and scope

Prospects choose between Anata's agency execution and platform access with Anata onboarding. Sales staff see those same standard offers in intake. Keep the existing design: offer cards, order, first-card emphasis, typography, colors, spacing, editor accordions, inclusion controls, and three next-step tiles. The image mockup establishes the offer distinction, not a new layout.

Use the user's spelling **DYI**. Do not rename it to DIY.

## Current implementation

- `sales_support_agent/services/deck/formatting.py`: `DEFAULT_CUSTOM_OFFERS`, legacy offer-key mapping, and custom-card normalization supply the deck defaults. Current offers are Channel management and Commission Model + Shipping OS.
- `sales_support_agent/services/admin_dashboard.py`: the Manual intake contains hardcoded offer editors; Digital Shelf reuses the Manual builder's offer UI. Update both entry points and verify their serialized payloads.
- `sales_support_agent/services/deck/service.py`: renders Proposed offers and next step and the three next-step tiles.
- `sales_support_agent/services/deck/rendering.py`: renders offer rows, defaults all cards to a three-month minimum, and uses the Anata contact URL for every CTA.
- `sales_support_agent/services/deck/story.py`: produces the companion Markdown offer section and next-action copy.

## Exact standard offer copy

| Existing field | DFY | DYI |
| --- | --- | --- |
| Title | DFY | DYI |
| Description | Our full agency service. Anata manages your Amazon marketing and operations, including creative, listing support, and advertising management. | Use the platform we use to manage your marketing and operations. Anata onboards your team, then you manage day-to-day execution yourself. |
| Price label | Monthly retainer fee | Platform subscription |
| Price | $3,000 | To confirm |
| Commission label | Commission on growth | Onboarding fee |
| Commission value | 5% | To confirm |
| Baseline label | Commission baseline | Execution owner |
| Baseline value | $10,000 | Your team |
| Bonus / note | TikTok Shop Support | Anata-led onboarding |
| Term | Preserve existing DFY behavior | To confirm |
| CTA text | Schedule kickoff | Discuss platform signup |

DFY retains current commercial defaults. DYI must not inherit the old offer's $0 retainer, 10% commission, Shipping OS requirement, or the renderer's three-month minimum.

Keep the existing card field structure for this small change. The label/value fields already support these details; update the visible intake field captions for DYI so staff see Subscription price, Onboarding fee, and Execution owner instead of agency terminology. No new platform-name, signup-URL, or onboarding-scope form controls in this phase. Keep descriptions and commercial details editable per prospect.

## Section and next-step copy

Keep eyebrow **Proposed offers and next step** and heading **Choose your engagement**.

Replace the caption with: **Choose full agency support with DFY, or platform access with Anata onboarding through DYI.**

Keep the three existing tiles:

1. **Pick your engagement** — Choose DFY for Anata-managed execution or DYI to run the platform yourself with our onboarding. Link text: **Discuss your engagement**.
2. **Why now** — Preserve the existing prospect-specific impact copy. No changes to market analysis or growth calculations.
3. **What happens next** — DFY: align scope and schedule kickoff. DYI: confirm your platform subscription and schedule onboarding, then your team takes over execution.

Ensure the offer section does not reuse agency-only `why_anata_summary` text that promises listing rewrites or campaigns for DYI. Scope this adjustment to the closing section; do not rewrite analysis elsewhere. Mirror the engagement distinction in Markdown next-action copy.

## Workflow and compatibility

Both standard cards are included by default in Manual and Digital Shelf intake, in DFY then DYI order. Staff may edit or exclude either using existing controls, then generate a deck through the existing workflow.

Preserve current include-slide behavior, custom offers, route contracts, validation, loading and error behavior, and responsive layout. Keep old offer identifiers accepted for compatibility. Do not rewrite previously saved deck payloads or regenerate historical deliverables as part of this change.

If a small optional term/CTA field is needed to prevent DYI inheriting agency copy, carry it through intake serialization and normalization. Do not derive the operating model solely from an editable title. Existing custom and historical cards retain their prior fallback behavior.

No new dependency, migration, analytics event, or styling change is required. The existing contact destination remains until a verified signup URL is provided. Do not label a contact link as direct platform signup.

## Unresolved details and implementation defaults

Platform name, subscription price, onboarding fee, and signup URL are not provided. Use generic platform wording, **To confirm** commercial values, and the current contact destination. These defaults allow implementation without inventing terms. Actual self-service signup integration is outside this copy update.

## Acceptance and validation

- New Manual and Digital Shelf intakes show DFY and DYI with matching standard copy and both inclusion controls enabled.
- Generated HTML and Markdown reflect the selected offers and staff edits, with no old second-offer sales copy in new defaults.
- DYI shows subscription, onboarding, and customer execution ownership; it does not display agency commission or minimum-term defaults.
- DFY keeps its existing $3,000, 5%, and $10,000 defaults.
- Include/exclude controls and custom offers still work. Optional term/CTA metadata survives intake serialization and normalization where introduced.
- Existing saved offers are not silently converted.
- Verify the closing slide and intake at desktop and narrow widths, plus deck print output: same layout and styling, no clipping from new copy, labels still associated with controls.
- Run focused deck generation and affected admin tests. Add regression coverage for default offers, custom normalization, and DYI term handling where behavior changes.

Implement only the affected deck/default/intake files and focused tests. Follow the repository deployment requirements after implementation and validation, keeping unrelated working-tree changes out of the deployment commit. This spec itself does not deploy application changes.
