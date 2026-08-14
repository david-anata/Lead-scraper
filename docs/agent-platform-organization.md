# Agent platform organization

Status: implemented navigation and prospecting workspace contract

## Operator model

Agent navigation exposes durable workspaces, not every implementation route.
Routes hidden from persistent navigation remain available from the workspace
that owns them, so permissions and existing links continue to work.

## Sales

- **Sales Overview**: daily owner priorities and source freshness
- **Pipeline**: deals and opportunity state
- **Rep Accountability**: ownership and follow-through
- **Prospecting Performance**: outbound outcomes and sending health
- **Company Library**: stored companies, filters, fresh sourcing, and Clay export
- **HubSpot Fixes**: records requiring a manual CRM correction
- **Sales Assets**: decks and sales collateral

Lead Operations and fresh-company sourcing are reached from Company Library.
They remain separate routes because they administer sourcing rather than work
with stored companies.

## HR

Persistent HR navigation is limited to daily workspaces: Today, People, Time &
Leave, Payroll, Compliance, and Manage. Setup, teams, contractors, offboarding,
reports, employee self-service, and other focused routes remain reachable from
their owning HR pages.

## Preserved contracts

- Route paths and permission keys are unchanged.
- Company export is read-only and does not mark a company contacted or delivered.
- Sales, outbound, HR, finance, fulfillment, and integration business rules are unchanged.
- Existing deep links continue to resolve.

## Company Library acceptance

The workspace must provide:

1. a visible company count and summary;
2. search plus tier and niche filters;
3. a complete Clay-ready CSV export;
4. a direct path to find fresh companies;
5. direct paths to sourcing administration and prospecting performance;
6. contained horizontal table scrolling; and
7. a usable phone layout without hiding the primary actions.
