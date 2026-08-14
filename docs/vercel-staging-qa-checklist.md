# Agent staging QA links

Release: `ab8d145`  
Deployment: `dpl_Frrh3wxQTPVUWdMQ7jUMbvLDCF5o`  
Scope: desktop staging only; production is unchanged

Use the same four checks on every page: the global header spans the viewport, the content aligns to the shared grid, the active workspace/page is clear, and no content is clipped or horizontally scrolling.

## Start here

- [Workspace home](https://agent-staging.anatainc.com/admin): confirm the greeting, Needs you, up to six ordered shortcuts, Recent after visiting another page, and only authorized workspaces.
- [Health page](https://agent-staging.anatainc.com/): confirm the human-readable operational status and Open Agent link.

## Sales

- [Control Room](https://agent-staging.anatainc.com/admin/sales): confirm priority totals, owner actions, source freshness, and understandable empty/error states.
- [Deal Board](https://agent-staging.anatainc.com/admin/sales/deals): confirm totals agree with Control Room, columns remain readable, and status chips do not crowd the table.
- [Rep Accountability](https://agent-staging.anatainc.com/admin/sales/reps): confirm owner grouping, search/filter behavior, and overdue/review distinctions.
- [Fix Queue](https://agent-staging.anatainc.com/admin/sales/fix-queue): confirm synced-state band, filters, drafts, and task links.
- [Sales Decks](https://agent-staging.anatainc.com/admin/sales/decks/): confirm create/generate states, history, and failure receipts.

## Website Ops and Content

- [Website Ops Today](https://agent-staging.anatainc.com/admin/website-ops): confirm action counts match the queue/history and freshness is visible.
- [Website publishing](https://agent-staging.anatainc.com/admin/website-ops/content): confirm the revised name, publishing status, and action hierarchy.
- [Site health](https://agent-staging.anatainc.com/admin/website-ops/site-health): confirm issues are prioritized and source status is clear.
- [Website history](https://agent-staging.anatainc.com/admin/website-ops/reports): confirm report cards, empty state, and detail links.
- [Content engine](https://agent-staging.anatainc.com/admin/content): confirm Riverside input, approval state, channel readiness, and that distribution remains safely gated.

## Finance and company operations

- [Finance Today](https://agent-staging.anatainc.com/admin/finances): confirm posted cash, receivables, expected income, payments, and exceptions stay visually distinct; do not test money movement.
- [Building](https://agent-staging.anatainc.com/admin/building): confirm the extra navigation layer is understandable and assignments/actions are clear.
- [Advertising Audit](https://agent-staging.anatainc.com/admin/advertising/audit): confirm upload, review, and correction states.
- [Owner Overview](https://agent-staging.anatainc.com/admin/executive): confirm owner-only intent, source freshness, and that stale placeholder values are not presented as current truth.

## Fulfillment and HR

- [Prospects & Assets](https://agent-staging.anatainc.com/admin/fulfillment/sales): confirm the revised name, target/asset relationship, filters, and pipeline readability.
- [CS Action Queue](https://agent-staging.anatainc.com/admin/fulfillment/cs/): confirm priority, ownership, next action, and empty/error states.
- [CS Reports](https://agent-staging.anatainc.com/admin/fulfillment/cs/reports/): confirm report list/detail links and freshness.
- [HR Dashboard](https://agent-staging.anatainc.com/admin/hr): confirm employee totals, add-employee visibility, and all section links.
- [HR Time & PTO](https://agent-staging.anatainc.com/admin/hr/time): for an employee-mapped account, confirm compact clock status on Home matches the HR time record before using Clock in/out.
- [HR Reports](https://agent-staging.anatainc.com/admin/hr/reports): confirm all report links remain visible and no section menu is clipped.

## Report a problem

Leave a Vercel toolbar note on the exact page and include: what you expected, what happened, and whether it blocks work. Do not place credentials, employee data, customer data, or financial details in a note or screenshot.
