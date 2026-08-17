# Lead Ops download reliability

## Problem

Live QA confirmed that StoreLeads pulls and CSV generation succeed, but the
Lead Ops page does not reliably start browser downloads. A new pull stored 18
companies and its preview worked, while both the individual and bulk download
clicks produced no browser download. The bulk action currently assigns
`location.href`; the links do not explicitly declare download intent.

## Users and outcome

An authenticated Sales operator must be able to pull companies and save the
resulting CSV without understanding browser routing or leaving Lead Ops.

## Scope

- Make **Pull now** explicitly download the generated CSV.
- Make each available Recent pulls **Download** action explicitly download its
  CSV.
- Make **Download selected CSV** a native download link whose URL tracks the
  selected pull IDs and duplicate preference.
- Keep preview, selection, deduplication, CSV columns, suppression behavior,
  StoreLeads queries, delivery settings, and unavailable historical pulls
  unchanged.

## Interaction states

- With no selection, the bulk download action is visibly and semantically
  disabled.
- With a selection, its URL includes the selected run IDs and duplicate choice.
- Changing the duplicate choice immediately updates that URL.
- Pull, individual, and bulk actions use native same-origin links with explicit
  download intent, preserving keyboard access and working without a page
  redirect.
- Existing empty, unavailable, provider-error, and permission behavior remains
  unchanged.

## Acceptance criteria

1. Clicking **Pull now** produces a browser download when the provider returns
   a CSV.
2. Clicking an available row's **Download** produces that pull's CSV.
3. Selecting one or more pulls enables **Download selected CSV** and clicking it
   produces the combined CSV.
4. The combined URL changes when **Include duplicates** changes.
5. No download action navigates the Lead Ops document away from the page.
6. All actions are keyboard reachable, have visible focus, and expose correct
   link semantics.
7. Desktop and phone layouts contain the selection action without clipping or
   global horizontal overflow.

## Validation

- HTML and route tests for download attributes, enabled/disabled bulk state,
  and generated URLs.
- Existing outbound export and pipeline regression suites.
- Production browser QA using the existing 18-company test pull: individual
  download, preview, bulk download, page URL stability, console errors, and
  desktop/phone layout.

## Rollout

Deploy normally. No migration or data backfill is required. Historical pulls
without stored companies remain unavailable by design.
