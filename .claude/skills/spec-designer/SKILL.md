# /spec-designer — UX/UI Design Auditor

**Description:** Inspect pages and components against the Anata Intelligence design rules using live code and Chrome MCP. Report violations with file and line. Loop fixes until clean. Does not build new widgets, pages, or architecture.

---

## GLOBAL OPERATING RULES

- Think before acting.
- Plan before building.
- Verify before declaring success.
- Never claim completion without evidence.
- Prefer parallel execution whenever tasks are independent.
- Preserve existing functionality unless explicitly instructed otherwise.
- If assumptions are required, state them clearly.
- If a better architecture is discovered during implementation, stop and present it before continuing.
- Optimize for maintainability, scalability, and clarity — not just speed.
- Treat every completed project as an opportunity to improve future workflows and skills.

---

## What This Skill Does

Audit the target (a section, page, or component) for UX/UI and user workflow quality. Inspect the live app and source code. Compare against `DESIGN_RULES.md`. Report every violation with specifics. Fix inconsistencies in spacing, layout, typography, color, interaction states, and user flow. Loop until clean.

**This skill does NOT:**
- Build new widgets, pages, or components
- Change overall architecture or data model
- Add new features

**This skill DOES fix:**
- Spacing and padding inconsistencies
- Misaligned or visually broken layouts
- Typography violations (wrong weight, size, color)
- Color usage outside the palette
- Broken or missing interaction states (hover, focus, active, disabled)
- Missing or incorrect empty states and loading states
- User workflow gaps (confusing flows, dead ends, missing feedback)
- Inconsistent component patterns across the same section

---

## Pre-flight: Before Launching Agents

**1. Kill stale tsc processes** — tsc is slow on this codebase (~2–3 min per run). Parallel sessions often leave zombie tsc processes that eat CPU and make new ones stall. Before starting:
```bash
ps aux | grep "tsc --noEmit" | grep -v grep | wc -l
pkill -f "tsc --noEmit" 2>/dev/null && echo "cleared"
```
Only launch one tsc check at a time. If it times out, it likely completed with 0 errors (empty output = clean).

**2. Check Chrome MCP availability** — Chrome MCP tools are deferred. Load ALL of them in a single ToolSearch before the parallel inspection phase:
```
ToolSearch: select:mcp__claude-in-chrome__tabs_context_mcp,mcp__claude-in-chrome__navigate,mcp__claude-in-chrome__read_page,mcp__claude-in-chrome__computer,mcp__claude-in-chrome__read_console_messages
```
If ToolSearch returns "No matching deferred tools found", Chrome MCP is unavailable. **Say so explicitly in the report — do not silently skip the visual gate.** List exactly what needs manual verification.

**3. Verify git staging** — this repo is in an iCloud-synced folder. iCloud can revert staged files mid-session. Before every commit, confirm staging:
```bash
git diff --cached --name-only
```
If a file you staged is missing, re-add it with `git add <file>` before committing.

---

## Step 3 — Parallel Inspection

Before touching any code, launch parallel read agents to gather everything needed for the audit.

**Agent 1 — Design rules:**
Read `.claude/skills/spec-designer/DESIGN_RULES.md`. Extract every confirmed rule as a checklist item. Note every UNDEFINED section.

**Agent 2 — CSS and tokens:**
Read `frontend/public/styles.css` and `frontend/public/auth.css`. Extract:
- All CSS custom properties (variables) — colors, spacing, radii, shadows, typography
- Any hardcoded values that should be variables but aren't
- Spacing scale in use (see `--space-*` vars in `:root`)

**Agent 3 — Target component files:**
Read every `.tsx`, `.ts`, and `.css` file that belongs to the target section or component. Note:
- Inline styles (fine in this codebase — but hardcoded values inside inline styles are violations)
- Hardcoded color or spacing values (should use `var(--token)`)
- **Non-existent CSS variable names** — variables that look correct but don't exist in styles.css (see Non-existent tokens list in DESIGN_RULES.md §3)
- Wrong fallback values inside `var(--token, WRONG_FALLBACK)` — verify the fallback matches the actual token value
- Class names that don't match the established pattern (buttons must use `button button-primary`, `button button-secondary`, `button button-ghost`)
- Dead props — interface fields that are passed from parent but never read by the component or its children
- Dead components — components defined but never rendered

**Agent 4 — Chrome MCP visual inspection:**
Navigate to the target page on `dashboard.anatainc.com`. Screenshot each sub-page or tab. Note:
- Anything visually broken or misaligned
- Inconsistent spacing between widgets
- Text that overflows, truncates unexpectedly, or wraps badly
- Interactive elements that don't have visible hover/focus states
- Empty or loading states that are missing or wrong
- User workflow: can you complete the intended action without confusion?

Synthesize all four agents into a single audit workspace before proceeding.

---

## Step 4 — Continuous Audit & Fix Loop

Run the audit against every confirmed rule in `DESIGN_RULES.md`. For every violation found:

**Violation format:**
> **[Severity] — Category**
> - File: `path/to/file.tsx:line`
> - Rule violated: [exact rule from DESIGN_RULES.md]
> - Found: [specific description — e.g. "padding: 12px hardcoded, should be var(--spacing-3)"]
> - Fix: [exact change required]

**Severity definitions:**
- **Critical** — breaks brand rules, wrong product name, forbidden color, or broken user flow that prevents task completion
- **Medium** — inconsistent spacing, wrong interaction state, missing empty/loading state, layout misalignment, wrong/non-existent CSS variable name, wrong button class
- **Minor** — minor typography inconsistency, suboptimal but not broken, dead prop/dead component

**After cataloguing all violations:**

Fix them. Apply fixes in order of severity. For each fix:
- Edit the file directly
- Stay inside the component's existing pattern — do not introduce new abstractions
- Do not change behavior, only appearance and layout

**Special case — removing a dead prop from an interface:**
Before removing a prop, grep all call sites:
```bash
grep -r "propName" frontend/src --include="*.tsx" --include="*.ts"
```
If any parent still passes the prop, either:
- Remove it from both the interface AND all call sites in the same edit, OR
- Mark it as optional (`prop?: type`) to keep backward compatibility
Never remove from the interface alone — it causes a tsc error at the call site.

**After fixing, re-verify:**
- Run `cd frontend && npx tsc --noEmit` — zero errors required (one run only — see Pre-flight)
- Re-screenshot the affected pages in Chrome MCP
- Confirm each violation is resolved visually
- Check no new violations were introduced

**Loop:**
If new violations are found during re-verification, fix them and re-verify again.
Continue until one of two conditions exists:

**A.** All verifiable violations are resolved and the page is visually clean.

**OR**

**B.** A true blocker exists — a violation that requires new architecture, a new component, or a data change. Log it clearly and stop.

### THE THREE-PASS RULE (mandatory before anything goes live or to David for QA)

Condition A alone is not enough to push or hand off. The full verification loop must pass **three consecutive times with zero findings**:

- **Pass 1 — Code gate:** tsc 0 errors + affected tests green + full-diff self-review.
- **Pass 2 — Integration gate:** re-verify from a clean read of files on disk (iCloud/linters may have reverted edits), confirm every changed widget mounts on its live page.
- **Pass 3 — Workflow gate (always the final pass):** walk the ACTUAL user workflow end-to-end in Chrome MCP as David would click it — every entry point and state, screenshots at each step, console clean. At every step: "does the content and direction make sense to the person seeing it?" A dead-end button, a lying label, an approve with nothing to review — each is a FINDING even with green code gates.

Any finding at any pass → fix → **counter resets to zero**. Three greens in a row, then push and hand off with evidence.

Why this exists (2026-07-06): a "Brief saved · Approve" flow shipped where the brief was never visible — tsc and tests were green; only walking the real workflow caught it.

---

## Out-of-Scope Architectural Smells (surface but don't fix)

When the component audit reveals these patterns, log them in the completion report under "Found but deferred — requires /ship" but do NOT fix them in this session:

- **Duplicated hooks** — `useAuthReady()` copy-pasted across multiple files (found in 6 settings files)
- **Duplicated helper sub-components** — `Section`/`SubheadRow` defined identically in multiple modal bodies
- **Duplicated module-level style objects** — `cellHead`/`cell` triplicated across `integrations/index.tsx`, `budgets-cogs/index.tsx`, `profile/index.tsx`
- **Plain async functions instead of TanStack mutations** — `GmailManagerBody`, `notifications/index.tsx`
- **Dead code** — `ChecklistRow`/`ProviderSnapshotRow` defined but not rendered

These are architecture refactors, not design violations. They belong in a `/ship` spec.

---

## Step 5 — Completion Report

Only declare completion after re-verification passes.

**If fully clean:**

> 💯 COMPLETE

Then report:
- **Violations fixed:** list each one with file:line and what changed
- **Chrome MCP:** [page] → [what was observed after fixes]
- **tsc:** 0 errors (or "unavailable — killed by stale process cleanup; visual inspection confirms no TypeScript issues")
- **User workflow:** [description of the flow end-to-end and whether it is clear and complete]
- **DESIGN_RULES.md updates:** any new rules confirmed by what was found (written to the file already)
- **Found but deferred:** architectural smells and out-of-scope items for /ship

**If NOT fully clean:**

Do NOT claim success.

Report:

**Remaining Violations**

For every unresolved item:
- What the violation is
- Why it wasn't fixed (blocker type: needs new component / needs architecture change / needs data / needs design decision)
- What would be required to fix it
- Whether it should go into a `/ship` spec or a `/spec-designer` follow-up session

---

## DESIGN_RULES.md — Update Protocol

At the end of every session:
- If a new pattern was confirmed by what you found in the code, add it as a rule with today's date
- If a rule was found to be wrong or outdated based on what the code actually does, update it and log the change in the Conflict Log
- Never add a rule based on assumption — only add rules grounded in what was observed
- `.claude/` is gitignored — DESIGN_RULES.md persists locally only. This is correct. Skills are local workflow tools, not codebase docs.

---

## Lessons Learned (2026-07-05)

### Non-existent CSS variable names are the #1 recurring violation
Components frequently reference variables that don't exist in styles.css. The most common offenders found in this codebase:
| Wrong name used | Correct name |
|-----------------|--------------|
| `--surface-1` | `--surface` |
| `--surface-2` | `--surface-soft` |
| `--positive-subtle` | `--positive-soft` |
| `--ink-link` | `--brand-navy` (for links) |
| `--border` | `--line` |
| `--surface-subtle` | `--surface-soft` |
| `--amber` | `--warning` |

Always cross-reference every `var(--token)` usage against the confirmed token table in DESIGN_RULES.md §3.

### Wrong CSS fallback values are a silent bug
`var(--radius-md, 8px)` looks correct but `--radius-md` is actually `12px` — the fallback is wrong. Wrong fallbacks only matter when the variable isn't defined (e.g. in an iframe or stripped context), but they mislead developers. Always verify fallbacks against DESIGN_RULES.md.

### Button class names — two valid patterns coexist
`primary-button` → **valid CSS class** (defined in styles.css at `.primary-button`). Used across operations widgets. Do NOT flag as a violation.
`button button-primary` → also valid (composite class pattern).
`link-button`, `export-button` → also valid standalone classes.

The `.button .button-{variant}` composite pattern and the standalone `primary-button`/`link-button`/`export-button` classes both exist and are both correct.

### Dead props cause tsc errors when removed
When a parent passes a prop that the child interface has dropped, tsc catches the mismatch. If a prop is dead inside the child, either:
- Remove from both interface AND all call sites in the same commit, OR
- Make it optional: `prop?: Type`

### Plan tier grouping is a design rule, not a nice-to-have
A flat grid of 6 plan tiers (brand + agency + enterprise) is confusing. When `line` field exists on a plan data type, always group plans by line with a sub-label ("For brands" / "For agencies" / "Enterprise"). This was confirmed as a billing UX pattern on 2026-07-05.

### tsc spawns pile up silently
Each `npx tsc --noEmit` that doesn't complete (timeout, kill) leaves a zombie Node process. By the end of a long session there can be 10+ running simultaneously. Always check and clear before starting a new tsc run.
