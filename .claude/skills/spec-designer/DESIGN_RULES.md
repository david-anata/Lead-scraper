# Anata Intelligence — Design Rules
*Living document. Updated by /spec-designer after each session.*
*Last updated: 2026-07-06*

---

## 1. Brand Identity

**Product name:** Anata Intelligence (always title case, never "ANATA INTELLIGENCE" or "Anata" alone)
**Parent company:** Anata (secondary, rarely surfaced)
**"by Anata" rule:** Use sparingly, never in the default lockup
**Icon + wordmark:** The product icon and "Anata Intelligence" wordmark are a paired lockup — use them together; wordmark must also work cleanly beside the icon without depending on it

---

## 2. Brand Personality

**Must feel:**
- Intelligent
- Precise
- Premium
- Technical
- Calm
- Trustworthy
- Operator-grade

**Must NOT feel:**
- Playful
- Futuristic / sci-fi
- Crypto
- Corporate-boring
- Whimsical
- Soft
- Generic SaaS analytics
- Generic BI dashboard

---

## 3. Color Palette

**Primary palette:** Navy, blue, ivory
**Accent:** Crisp whites, controlled use of blue for interaction states
**Forbidden:** Purple, neon, futuristic gradients, warm/saturated colors as primaries

### Confirmed token values (from styles.css `:root`, 2026-07-06 full audit)
| Token | Value | Usage |
|-------|-------|-------|
| `--brand-navy` | `#2e6eb2` | Primary interactive color, borders, badges |
| `--brand` | `#85bbda` | Secondary brand blue |
| `--brand-deep` | `#5e9fc4` | Deeper brand blue |
| `--brand-soft` | `#e6f2f8` | Brand blue tint surface |
| `--brand-navy-soft` | `#5b94d1` | Softer brand navy |
| `--navy` | `#12233f` | Darkest navy (sidebars, deep contrast) |
| `--ink` | `#15273f` | Default text |
| `--ink-muted` | `#62738d` | Secondary/label text |
| `--ink-soft` | `#66758f` | Tertiary/placeholder text — lighter than `--ink-muted` |
| `--surface` | `#ffffff` | Card/panel background |
| `--surface-soft` | `#f7f9fc` | Highlighted card background |
| `--surface-muted` | `#eef3f9` | Muted surface background |
| `--bg` | `#edf2f8` | Page/workspace background |
| `--line` | `#d9e2ef` | Borders, dividers |
| `--line-soft` | `#eef1f6` | Subtle divider |
| `--line-strong` | `#bfccde` | Stronger border/divider |
| `--accent` | `#f0a85d` | Orange — semantic accent only (NOT for links or CTAs) |
| `--accent-soft` | `#fff0de` | Orange tint surface |
| `--positive` | `#2d9c67` | Success/green states |
| `--positive-soft` | `#e7f5ee` | Success background fill |
| `--negative` | `#d75c58` | Error/destructive states |
| `--negative-soft` | `#ffecea` | Error background fill |
| `--warning` | `#b98530` | Warning states |
| `--warning-soft` | `#fff4df` | Warning background fill |
| `--info` | `#2e6eb2` | Info states (= --brand-navy) |
| `--info-soft` | `#e6f2f8` | Info background (= --brand-soft) |
| `--overlay` | `rgba(0,0,0,0.25)` | Drawer backdrops, modal scrims — NEVER hardcode rgba(0,0,0,...) inline |
| `--shadow-panel` | `0 1px 0 rgba(20,39,63,0.04), 0 4px 12px rgba(20,39,63,0.04)` | Card shadow |
| `--shadow-sidebar` | `10px 0 24px rgba(80,57,26,0.1)` | Sidebar shadow |
| `--workspace-header-h` | `63px` | Workspace topbar height — use as top offset for fixed overlays |
| `--transition-fast` | `140ms ease` | Standard interaction transition |

### Link and CTA color rule (Confirmed 2026-07-05)
- Text links use `var(--brand-navy)` — never `var(--accent)` (orange) or any purple/indigo value
- `--accent` is orange (#f0a85d) — only for semantic accent uses, not links or navigation

### Color purpose — every color has ONE job (Confirmed with David 2026-07-07)
Reviewed the palette live with David. He chose to KEEP the current colors as-is (the warm accent is
intentional, not a bug). The rule going forward: **no color appears without a defined purpose, and a color
is never used outside that purpose** (e.g. never use warning-amber on a non-caution element, never use the
warm accent for a resting/idle state). Each token's single job:

| Color | Value | Its ONE job — the only reason to reach for it |
|-------|-------|-----------------------------------------------|
| `--brand-navy` | `#2e6eb2` | Identity + interaction: text links, primary buttons, active nav, focus rings. The calm, credible "intelligence" anchor. |
| `--accent` (warm) | `#f0a85d` | The **highlight of the moment**: active tab underline, progress-bar fill, the one thing on a view you should look at now. Intentional warm pop against the navy — energy, not alarm. Use sparingly; it loses meaning if everything is orange. |
| `--positive` | `#2d9c67` | Success / on-track / goal-met / positive delta. Only when something is genuinely good. |
| `--warning` (bronze) | `#b98530` | Caution / in-progress "Building" band / a real "needs attention but not broken." Never decorative. |
| `--negative` | `#d75c58` | At-risk / error / negative delta / destructive. Only when something is genuinely wrong. |
| neutrals (`--ink*`, `--line*`, `--surface*`) | — | Structure, text, and "leave-alone" states (e.g. the Avoid-in-PPC dot is neutral gray, NOT amber — "don't act" is neutral, not a warning). |

Test before using a color: *"What is this element's status or role, and does this color mean exactly that?"*
If the answer is "it just looked nice," use a neutral. Semantic colors (positive/warning/negative/accent)
must carry semantic meaning; decorative use dilutes them and is a violation.

### Non-existent tokens (do NOT use — confirmed absent 2026-07-06)
- `--surface-1` — does not exist; use `--surface`
- `--surface-2` — does not exist; use `--surface-soft`
- `--surface-subtle` — does not exist; use `--surface-soft`
- `--surface-raised` — does not exist; use `--surface-soft`
- `--surface-hover` — does not exist; use `--surface-soft`
- `--positive-subtle` — does not exist; use `--positive-soft`
- `--ink-link` — does not exist; use `--brand-navy` for links
- `--border` — does not exist; use `--line`
- `--amber` — does not exist; use `--warning`
- `--text-xl` — does not exist; use `--text-heading` (20px)
- `--brand-navy-light` — does not exist; use `--brand-navy`
- `--radius` (bare) — does not exist; use `--radius-sm` (8px), `--radius-md` (12px), `--radius-lg` (14px), or `--radius-xs` (6px)`

---

## 4. Typography

**Direction:** Legible, repeatable, implementation-friendly — not over-stylized custom lettering
**AI abbreviation:** If "Anata Intelligence" is typeset in a lockup, a subtle visual reinforcement of "AI" is acceptable but only if it does not compromise legibility
**Forbidden:** Decorative fonts, anything that reads as playful, anything requiring custom rendering to look right

### Confirmed type tokens (from styles.css `:root`, 2026-07-05)
| Token | Value |
|-------|-------|
| `--font-heading` | `"Montserrat", sans-serif` |
| `--font-body` | `"Roboto", sans-serif` |
| `--text-xs` | `11px` |
| `--text-sm` | `13px` |
| `--text-base` | `14px` |
| `--text-lg` | `16px` |
| `--text-heading-sm` | `18px` |
| `--text-heading` | `20px` |
| `--text-display-sm` | `24px` |
| `--text-display` | `32px` |

---

## 5. Audience & Context

**Users:** Amazon operators, ecommerce teams, brands, agencies
**Mental model:** Operator system — control, trust, actionability
**Messaging approach:** Lead with operator outcomes, not vanity metrics
**AI framing (acceptable):** "AI-guided operator workflows," "AI decision layer," "recommendations tied to action"
**AI framing (forbidden):** "Revolutionary AI," abstract productivity claims, vague automation promises

### Identifier hierarchy (Confirmed 2026-07-05)
- **Product title is primary** — always use the product name/title as the primary label in cards, headlines, tables, and recommendation copy. Operators know their products by name, not ASIN.
- **ASIN is supplemental** — show the ASIN as a secondary label (muted, small, below or beside the title), never as a standalone identifier in a headline or card.
- **Never surface raw ASINs in copy** — recommendation headlines, "Next action" titles, and "Risk exposure" headlines must use the resolved product title. A raw ASIN like 'b0dswk8pv5' in operator-facing text is a Critical violation.
- **Campaign names with embedded ASINs** — Amazon auto-campaign names follow the pattern `{title} | {ASIN} | {type} | {targeting}`. Always strip the ASIN and trailing segments when displaying campaign names in tables; show only the product title portion.
- **API enum values are never user-facing** — Amazon Ads match types (`TARGETING_EXPRESSION_PREDEFINED`, `TARGETING_EXPRESSION`, `BROAD`) must be mapped to human labels ("Auto", "Target", "Broad") before display.

---

## 6. Component & Layout Rules

*[Rules to be built out through /spec-designer sessions]*

### Loading states
- **Page / initial data load:** Skeleton placeholders — grey rounded rectangles matching the widget shell dimensions. No spinner. Layout is visible before data arrives. (Confirmed 2026-07-05)
- **User-triggered refresh (date change, filter, manual reload):** Spinner centered inside the widget shell. Shell border and title remain visible. (Confirmed 2026-07-05)

### Empty states
- **Priority order:** (Confirmed 2026-07-05)
  1. **Action CTA first** — if the user can resolve the empty state (missing config, no budget set, no COGS, etc.), show a blue "Set up in [destination] →" CTA inside the widget shell. Never leave the widget blank when a user action would fix it.
  2. **Neutral display second** — if no user action can resolve it (data simply doesn't exist yet), render the widget normally with "—" for values. No error icon, no alarming language.
- **Locked / unavailable state:** Always include an upgrade CTA. Never show a locked state without a path to unlock. (Confirmed 2026-07-05)
- **Never return `null` from a widget component** — returning null silently hides data problems. Always render WidgetShell with an explanatory state instead. (Confirmed 2026-07-05)
- WidgetShell renders EmptyState in place of children when `empty=true` — children are fully replaced, not rendered alongside. (Confirmed 2026-07-05)
- Always include `!creating` (or equivalent optimistic-state flags) in the `empty` condition so user-initiated flows don't get suppressed by EmptyState. (Confirmed 2026-07-05)
- EmptyState visual: dashed circle icon + message text, compact body height. (Confirmed 2026-07-05)
- Empty message must explain WHY there is no data and WHAT would fix it (e.g. "No reimbursement events posted in this window — use Operations to investigate discrepancies"). (Confirmed 2026-07-05)

### Status badges
- Status badge palette must stay within the blue/navy/teal family plus green (positive) and amber (warning/draft) and gray (closed/neutral). (Confirmed 2026-07-05)
- Forbidden in status badges: purple, violet, indigo — even at low opacity. (Confirmed 2026-07-05)
- Badge colors confirmed in FbaShipmentBuilder:
  - draft → amber (`rgba(245,158,11,0.12)` / `#9a6700`)
  - planned → brand-navy (`rgba(46,110,178,0.10)` / `var(--brand-navy)`)
  - shipped → positive-green (`rgba(45,156,103,0.12)` / `#1f7a4d`)
  - receiving → teal-blue (`rgba(8,145,178,0.10)` / `#0a7490`)
  - closed → surface-muted / ink-muted

### Buttons and interactive labels
- No emoji in button labels or link buttons. Emoji are forbidden in all action controls. (Confirmed 2026-07-05)
- State changes in button labels (e.g. "Share" → "Shared") use text only, not emoji indicators.

### Overlay / scrim token (Confirmed 2026-07-05)
- `--overlay: rgba(0,0,0,0.25)` — use for drawer backdrops, modal scrims, and panel overlays. Never hardcode `rgba(0,0,0,...)` inline.
- Backdrop `<div>` must include `role="presentation" aria-hidden="true"` so screen readers skip it.

### ARIA patterns (Confirmed 2026-07-05)
- Buttons that open a `role="dialog"` popup must set `aria-haspopup="dialog"` — not `"true"` (which implies listbox).
- Buttons that open a `role="menu"` dropdown must set `aria-haspopup="menu"`.
- Duplicate visible labels (`title` + `aria-label`) are acceptable but `aria-label` always takes precedence over `title` for AT — use `aria-label` as the canonical accessible label.
- White checkmark/icon color must use `var(--surface)` not `#fff`.

### Hardcoded colors — forbidden list (Confirmed 2026-07-05)
- `#ef4444` — use `var(--negative)` instead (error/danger text).
- `#fff` — use `var(--surface)` for white surfaces.
- `rgba(0,0,0,...)` inline — use `var(--overlay)` for scrims; `var(--shadow-panel)` or `var(--menu-popout-shadow)` for shadows.

### Tab navigation consistency (Confirmed 2026-07-05)
- Tab strip labels must exactly match the corresponding sidebar nav labels. Abbreviations (e.g. "P&L" for "Profit & Loss") are acceptable only if the sidebar also uses the abbreviation.
- Tab order must match sidebar order. The sidebar is the canonical reference — update tab arrays in React to match.

### Tables (Confirmed 2026-07-05)
- **Row actions:** Always visible — never hide behind hover. Use a "..." (ellipsis) button in a dedicated Actions column at the far right. Action menus are grouped by semantic category (STATE / BUDGET / BID / etc.) with section headers.
- **Sort indicators:** Always show the active sort direction arrow in the column header. Arrow on hover for unsorted columns is acceptable, but the currently sorted column always shows its direction.
- **Row density:** Comfortable rows (not compact). Operator-grade does not mean cramped — data should be readable without hover to reveal context.
- **Delta badges:** Colored directional badges (green up-triangle / red down-triangle) inline beneath the primary value. "—" for null or zero-comparison values.
- **Sortable header:** Bold column label + sort arrow. Non-sortable columns have no arrow.

### KPI strips (Confirmed 2026-07-05)
- **Layout:** Large primary value (`--text-display` / 32px) + delta badge inline + muted context line below (e.g. "vs $39,414 in prior 30d").
- **Delta badge color — context-sensitive:** Green = improvement for that metric, red = deterioration. Direction (up/down) alone does not determine color. Examples: Fees up = red (bad), Revenue up = green (good), Refunds up = red (bad), CVR up = green (good). Each metric must declare its own `positiveDirection` (up or down) so the badge renders correctly.
- **positiveDirection rule (Confirmed 2026-07-05):** Every KPI metric that feeds a delta badge must declare `positiveDirection: "up" | "down"`. Badge color is determined by whether the change is an improvement, not whether the number went up. Metrics where lower is better (Fees, Refunds, ACoS, TACoS, Returns rate) must declare `positiveDirection: "down"`.
- **No prior period:** Show badge as "—". Never hide the badge entirely — its absence would look like a bug. Never show "0%" for a missing comparison.

### Spacing system (Confirmed 2026-07-05)
Spacing variables from `styles.css`:
| Token | Value | Use |
|-------|-------|-----|
| `--space-xs` | `4px` | tight gaps, inline |
| `--space-sm` | `8px` | intra-card gaps |
| `--space-md` | `12px` | card grid gaps |
| `--space-lg` | `16px` | section margins, padding |
| `--space-xl` | `20px` | section gaps |
| `--gap-widget` | `14px` | widget-to-widget gap |
| `--gap-page` | `20px` | page-level section gap |

**Rule:** Always prefer spacing variables over hardcoded `px` values. Never use raw integers (e.g. `gap: 32`) — use `var(--space-*)` with a pixel fallback.

### Border radius (Confirmed 2026-07-05)
| Token | Value | Use |
|-------|-------|-----|
| `--radius-lg` | `14px` | large cards |
| `--radius-md` | `12px` | standard cards, panels, inputs |
| `--radius-sm` | `8px` | buttons, chips |
| `--radius-xs` | `6px` | small elements |

**Rule:** Pill shape (badges, status chips) uses `border-radius: 999px` — not a variable. Card and panel corners use `var(--radius-md)` (12px).

### Interactive states (hover, focus, active) (Confirmed 2026-07-05)
- **Focus ring:** Custom glow — `outline: none; box-shadow: 0 0 0 3px rgba(47, 111, 183, 0.16);`. Never a hard solid outline. Never browser-default blue ring. Never indigo/violet fallbacks.
- **Clickable table rows (hover):** Light brand-blue tint — `background: rgba(47, 111, 183, 0.08)`. Standard across all tables.
- **Primary buttons (hover):** Darkened background — `background: var(--navy); border-color: var(--navy);`. The navy deepens from `--brand-navy` (#2e6eb2) to `--navy` (#12233f) on hover.

### Error states (Confirmed 2026-07-05)
- **Widget-level error (API/spec failure):** Subtle inline message inside the widget shell — e.g. "Unable to load data — try refreshing." Never a red banner that dominates the layout, never a silent blank. The widget shell stays visible; only the content area is replaced with the message.
- **Form / input validation:** Each widget handles its own validation inline. No global toast pattern mandated — the specific widget determines the right error placement (inline below the field, inline near the submit action, etc.). Consistency within a widget matters more than cross-widget uniformity.
- **Retry button rule (Confirmed 2026-07-05):** Show Retry ONLY when retrying could succeed — transient network/API errors (timeout, 500, 503, rate limit). Do NOT show Retry for structural errors ("No account connected", "Permission denied", "No data for this account") — those need a user action, not a reload.
- **No technical details in user-facing errors (Confirmed 2026-07-05):** Never surface endpoint paths, Python exception text, HTTP status codes, or stack traces. All user-facing errors must be plain English.
- **Error code mapping required (Confirmed 2026-07-05):** Backend errors must map to human-readable messages before reaching the UI. Until a formal map exists, fall back to: "Something went wrong loading [widget name]. Try refreshing or contact support if it persists." Never let raw exception messages reach the user.
- **Structural vs transient distinction (Confirmed 2026-07-05):**
  - Transient → show Retry: network timeout, 500/503 server error, rate limit
  - Structural → no Retry, show next-action instead: no account linked, missing permission, data gap in date range, account suspended

### Navigation (Confirmed 2026-07-05)
- **Active page indicator:** Inset left border only — `box-shadow: inset 3px 0 0 var(--brand-navy)`. No background fill on the active row. The border alone is the canonical "you are here" signal.
- **Collapsed section with active child:** The section header itself gets the active highlight (inset left border) to signal "the active page is inside this collapsed group." The individual page row is not visible but the section header inherits the active state.
- **Mobile / narrow viewport:** Hamburger menu. The sidebar is hidden by default on mobile and toggled via a hamburger control. No bottom tab bar.

---

## 7. Surface-Specific Rules

### Dashboard (dashboard.anatainc.com)
- Canonical production app — no domain migration
- Should feel aligned with the same design family as auth and marketing surfaces
- "More pop" than auth pages — but no fluff or startup gimmicks

### Auth surfaces (login, signup, forgot-password, reset-password, connect)
- Use product icon + "Anata Intelligence" wordmark lockup
- Light surface rendering rules apply

### Shared pages (shared-po, shared-restock, shared-strategy)
- Same brand lockup as auth surfaces
- No separate brand identity for guest/shared views

### Marketing site
- Product-brand-forward, not corporate-parent-forward
- Same design family as the product app — not a disconnected marketing identity
- CTA targets: dashboard.anatainc.com paths only

---

## 8. Conflict Log

*Conflicts resolved by /spec-designer are recorded here with the date and decision.*

| Date | Rule A | Rule B | Resolution |
|------|--------|--------|------------|
| — | — | — | — |

---

## 9. Data Honesty Rules (Profit-specific, Confirmed 2026-07-05)

### Data confidence banner
- **Pattern: Option B — Global persistent strip** above the KPI strip on every Profit sub-page. Shows a consolidated data confidence summary: COGS mode, known date gaps, ad spend coverage start date.
- **Priority mandate:** The goal is to make this banner unnecessary by completing data backfills. The banner is an honest interim state, not a permanent feature. It should shrink and eventually disappear as gaps are filled.
- **Banner must be non-alarming** — use `--warning` amber tone, not `--negative` red. Phrasing: "Data confidence: COGS estimated · Finance events missing Dec'25–Feb'26 · Ad spend from Mar'26." Never use "ERROR" or "BROKEN" language.
- **Per-widget inline notes** remain for widgets with localized caveats (e.g. waterfall COGS note, margin trend COGS note) — the global banner does not replace per-widget context.

### Data backfill mandate
- **Critical project directive (Confirmed 2026-07-05):** All account data gaps MUST be filled. The Dec'25–Feb'26 financial event gap and any other historical gaps are not acceptable long-term. A backfill trigger system is required — an operator-accessible way to initiate a historical sync for any account.
- **Backfill trigger system requirements:** Must be accessible from Admin or Settings, must show progress, must be idempotent (safe to re-run), must not re-import already-present records.

---

## 10. Open Questions

- Table row pattern (hover state, sort indicator, action column) — UNDEFINED
- KPI strip layout (delta badge shape, up/down color) — UNDEFINED
- Focus ring: use `var(--brand-navy, #2e6eb2)` outline — never indigo/violet fallbacks. (Confirmed 2026-07-05)
- Error state pattern (inline vs. toast) — UNDEFINED

## Rule (confirmed 2026-07-08) — Public `shared-*.html` pages must load the platform design system
Standalone public share pages (`shared-po.html`, `shared-image-audit.html`, `shared-compliance-review.html`, `shared-voice-of-customer.html`) MUST, in their HTML shell: (1) `<link rel="stylesheet" href="/public/styles.css">` and `<link rel="stylesheet" href="/auth.css?v=...">` so the real design tokens + brand styles load (Vite bundles `/public/styles.css` into a hashed `styles-*.css` asset at build); (2) include the scroll-override inline `<style>` (styles.css locks `html,body{overflow:hidden}` for the SPA); (3) render the standard brand lockup via `<header class="auth-header"><a class="logo">…<img class="auth-logo-lockup">`. The React app's local `styles.css` may only lay out page-specific structure using `var(--token)` — NEVER a local `:root` override, hardcoded hex, or a `prefers-color-scheme` dark fork (the Anata platform is a single LIGHT theme; `--bg #edf2f8`, `--surface #fff`, `--ink #15273f`). Gold-standard reference: `shared-image-audit.html`. Violation found + fixed on the VoC share page 2026-07-08 (it self-authored a dark hardcoded-hex styles.css).
