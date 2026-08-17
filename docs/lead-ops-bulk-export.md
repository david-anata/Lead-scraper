# Lead Ops pull exports and team delivery

Lead Ops is the operational home for StoreLeads pull recipes, recent pull
results, and read-only delivery of those results to Anata operators.

## Pull result storage

Every new pull records its summary in `outbound_pull_runs` and its exact company
membership in `outbound_pull_run_leads`. This additive record is what makes an
individual pull or a selection of pulls reproducibly downloadable. For older
runs, Lead Ops attempts a one-time recovery from the company timestamp window,
recipe, and recorded fresh count. It enables the pull only when the recovered
row count exactly matches the recorded count; ambiguous runs remain unavailable.

Downloading never writes to `outbound_contacted_domains`, never triggers Clay,
and never changes a company's operational state. Combined exports deduplicate by
normalized domain by default, retain the newest selected record, and append pull
provenance columns. Operators can explicitly include duplicates.

Export metadata is recorded in `outbound_export_history`; full duplicate files
are not stored.

## Delivery settings

Authorized operators can configure email, Slack, or both, with delivery after
each pull or in the existing daily morning digest. Email and Slack credentials
remain deployment environment variables; the page stores only operator choices
and recipient addresses.

Email delivery includes a Clay-ready CSV attachment plus pull counts, status,
settings version, and an authenticated Lead Ops link. Daily digest email uses a
combined CSV of the ready companies. Slack includes the summary and secure page
link because its channel delivery remains message-only. Notifications never
contact prospects or alter suppression. Each provider attempt is written to
`outbound_delivery_history` with destination, target, status, and timestamp.

The Test action is clearly labeled in its message and exercises the same provider
path as automatic delivery.
