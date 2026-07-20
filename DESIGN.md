# Anata Product Design System

## Purpose

This is the visual and interaction contract for Anata Agent. It applies before
any external design skill or agent preference. The product is an operator
system: every major view must make it easy to see what is happening, what is
broken, and what should happen next.

## Authority Order

1. `AGENTS.md`: product, reliability, security, and deployment constraints.
2. This file: visual, interaction, accessibility, and operator-flow rules.
3. Approved page specifications and existing source-of-truth data contracts.
4. Installed skills, including Impeccable and Emil Kowalski's skills.
5. General agent defaults.

External skills are review and execution aids. They may identify drift but may
not replace the Anata brand, change the product register, add dependencies, or
rewrite the information architecture without an approved product decision.

## Brand And Tokens

- Reuse the shared Anata brand primitives and `sales_support_agent/static/finance.css`
  tokens. Do not introduce raw colors, font families, shadow values, or radii
  when an existing token or component rule already fits.
- The established pale blue-to-warm-neutral Finance background is intentional.
  It is not a generic "AI cream" default and may not be replaced merely to
  satisfy an external anti-pattern rule.
- `Montserrat` is reserved for concise headings, labels, and controls;
  `Inter`/`Segoe UI` is for operational reading. Maintain high contrast for
  body copy and metadata.
- Status color carries meaning only: teal for verified/positive, amber for
  caution, red for blocked/required attention, and sky for neutral context.
  Never use color as the only status signal.

## Layout And Hierarchy

- Finance Control is one desktop page. The primary flow is source readiness,
  decision trust, cash posture, trajectory, then the operator queue.
- Prefer a small number of purposeful regions over repeated generic card grids.
  A bordered/tinted panel is valid when it groups a decision, source, or action;
  do not nest panels merely for decoration.
- Keep financial controls and table columns stable. Long queues must use filters,
  row limits, pagination, and a visible result count rather than an endless
  scroll.
- Make the current action discoverable without requiring the operator to infer
  it from charts or raw data. Each alert must end with a route to resolve it.
- Titles may use an eyebrow only when it identifies a real product subsystem,
  such as `Finance Control` or `Operator queue`. Do not add an eyebrow to every
  section by reflex.

## Financial Semantics In The UI

- Label every number by evidence class: actual cash, confirmed receivable,
  expected income, required payment, or manual exception.
- Cash on hand comes from posted bank/QBO evidence, never from a forecast.
- A ClickUp-completed task is not automatically a paid obligation. The UI must
  show it as reconciled only after matching posted Bank CSV or QuickBooks
  evidence exists.
- When sources conflict or evidence is missing, pause the affected decision and
  explain the concrete source action needed. Never hide uncertainty to make the
  page appear complete.
- Charts explain cash timing. They cannot be the only place that exposes a
  material risk, funding gap, missing income configuration, or data-quality
  blocker.

## Interaction And Motion

- Financial review is a high-frequency task. Do not animate figures, table rows,
  filters, or keyboard-driven controls merely for delight.
- Motion is allowed only for state continuity: modal/dialog entry, source refresh
  feedback, non-blocking confirmation, and intentional disclosure. Use specific
  properties, 120-240ms durations, and responsive ease-out curves.
- Every motion treatment must preserve visible content without JavaScript and
  include a `prefers-reduced-motion` alternative.
- Use native semantics first: buttons, links, labels, dialogs, tables, live
  regions, and focus management. A control must remain usable without hover.

## Quality Gate

Before shipping a visual change, verify:

1. The page still answers cash, incoming money, required payments, broken
   sources, and next action within one scan.
2. Desktop view at 1280px and 1440px has no clipping, accidental horizontal
   scroll, obscured controls, or unreadable dense areas.
3. Keyboard focus, contrast, and reduced-motion behavior work.
4. Data labels distinguish actual, confirmed, expected, and untrusted values.
5. The change does not add an unapproved UI dependency such as GSAP, a component
   framework, or a second design system.

## External Skill Adaptations

- Use Impeccable for product-mode critique, accessibility checks, hierarchy,
  typography, and anti-slop detection. Treat detector findings as review items,
  not automatic failures, when they conflict with established Anata tokens.
- Use Emil's skills for purposeful motion review. Do not add motion merely
  because an animation opportunity exists.
- Do not install Taste Skill globally. Its experimental v2, dark-mode bias, and
  prescribed motion stack are not authoritative here. Reuse only compatible
  ideas through this document after a deliberate review.
