# Finance handoff, 8 August 2026

Written for whoever picks this up next. Everything below was verified against the
live app at `agent.anatainc.com` on the day, not inferred from code.

Last commit: `b2b06b3` "Finance: take the ClickUp estimates out of the numbers".

---

## 1. The one thing to understand

**Plaid is the source of truth. ClickUp is dead.**

ClickUp only ever held *estimates*, never schedules. David confirmed this
explicitly. Its rows had been driving the calendar, required-out and the paydown
plan for weeks after everyone considered it retired, because turning the sync off
only stops new rows arriving. The rows already written keep counting forever.

204 ClickUp rows were archived on 8 August. The cutover page now reads "Safe to
switch off". Do not reintroduce ClickUp as an input to any number.

If you find yourself writing "we could fall back to the ClickUp value", stop.

---

## 2. What was built this session

| File | What it does |
|---|---|
| `services/cashflow/rent_paydown.py` | Dated plan for paying down one large recurring bill |
| `services/cashflow/charge_drilldown.py` | The charges behind a weekly total, with cadence answers |
| `services/cashflow/cutover.py` (extended) | `archive_clickup_ledger()`, reversible |
| `services/cashflow/cash_calendar.py` (extended) | Next-week headline, paydown block, clickable totals |

Tests: `test_finance_rent_paydown.py`, `test_finance_charge_drilldown.py`,
`test_finance_charge_routes.py`, `test_finance_calendar_headline.py`,
`test_finance_clickup_archive.py`, `test_finance_bookkeeping_query_budget.py`,
`test_finance_no_leave_warning.py`.

---

## 3. Next tasks, in David's priority order

### 3a. Payroll from the HR module

David wants payroll and his own pay driven by HR, not estimated from bank history.

**This is connectable, not a rebuild.** `models/hr.py` already has `HRPayrollRun`
with `pay_date`, `total_net_cents`, `total_gross_cents`, `employee_count` and a
`status` of `draft|processing|completed|partial|failed`.

The work is to read upcoming runs and surface them as real commitments in the
forecast. Open questions David has not answered:

- Does a `draft` run count as committed cash, or only `processing` and later?
- What happens when HR says one thing and the bank later pays another? The bank
  should win after the fact, but the forecast needs the HR figure beforehand.

Note the bank already detects "Payroll Intuit" at about $3,186 a month, paid in
pieces. So HR should *replace* that detection for payroll, not sit alongside it,
or payroll gets counted twice. That double-count is the main risk here.

### 3b. Vendors with contract terms

David's words: *"I would like to create vendors, contracts, terms etc. finance
should eventually be a full quickbooks parity."*

`finance_vendors` exists with `running_account` and `payment_method`. It does not
have contract references, term lengths, notice periods or renewal dates. This is
the largest remaining piece and deserves its own spec before any code.

### 3c. Credit card classified as a reserve

Citi Simplicity Card (••6352) carries a balance of $268.78 and is classified
`cash_role = 'reserve'`, so **money owed is being counted as money held**. Small
today, wrong in principle, and it grows. Cards probably need a third role rather
than being forced into spendable/reserve/excluded.

### 3d. The finance home page takes about 2.8 seconds

Measured. It is *not* compute: the whole control build is ~106ms at David's data
size. It is the number of separate round trips per request, chiefly `nav_counts`
calling five independent expensive builders. Count queries, do not profile
Python. This is the single biggest quality-of-life fix available.

### 3e. Duplicate transactions across feeds

The same transaction arrives from `plaid`, `qbo_bank` and `csv` and is counted
more than once. The bill audit reports it; nothing fixes it. Fixing it rewrites
history, so it needs its own careful session.

---

## 4. Traps that will cost you hours

These are all real, all hit during this session.

**`datetime` is a subclass of `date`.** A helper that tests `isinstance(value,
date)` first returns a timestamp untouched, and the next comparison against a
plain date raises `TypeError`. The live database returns timestamps for date
columns; every local fixture used plain dates and ISO strings. This shipped
broken to production and no test caught it. Always test `datetime` first. See
`rent_paydown._as_date`.

**Never key a cache on `id()`.** CPython hands a dead object's address to the
next allocation, so an id-keyed cache eventually tells a brand new engine its
schema already exists. Use `weakref.WeakSet`. See `vendor_aliases._SCHEMA_READY`.
The symptom was one finance test failing under some orderings and passing under
others.

**Hoist alias lookups out of loops.** `bookkeeping.merchant_key()` resolves vendor
aliases, which queries the database. Called per row it was two round trips per
transaction: ~3,500 for a 1,700 row queue. `test_finance_bookkeeping_query_budget.py`
guards this by counting statements, not by timing.

**`git checkout --` destroys uncommitted work.** Bit me twice. Commit before any
experiment that reverts files.

**`git stash` plus a command timeout loses work.** The stash survives but the pop
never runs. Use a detached worktree for baselines instead:
`git worktree add --detach /tmp/base origin/main`.

**macOS has no `timeout` command.** `timeout 600 cmd` fails with "command not
found" and can look like the gate itself failing.

**Re-baseline before every zero-net-regression claim.** `origin/main` moves
several times a day from parallel sessions. A stale baseline invents both
regressions and fixes. Compare failure *sets*, never counts.

**Production is Vercel, and it does not build from `main`.** As of 2026-08-19
`agent.anatainc.com` is served by the Vercel project `anata-agent-staging`,
built from the branch `codex/vercel-agent-duplicate`. Work merged into `main`
is not live. Confirm with `git log -1 origin/codex/vercel-agent-duplicate`
before assuming anything you can see in `main` is deployed. `render.yaml` is
still in the tree and is now history, not configuration.

**The Vercel entrypoint is not the file you think it is.** `app.py` imports
`sales_support_agent/main.py`. The root `main.py` is a second, much larger
FastAPI application that is imported for its helpers but never served. Anything
registered on its `@app.on_event("startup")` hooks does not run in production.
That is how the finance background loop went silent after the migration.

**Scheduled work is Vercel Cron, and it is allowlisted twice.** Schedules live
in `vercel.json` and route into `sales_support_agent/api/vercel_cron_router.py`.
A schedule stays inert unless `VERCEL_CRON_WRITES_ENABLED` is on *and* its job
name is in `VERCEL_CRON_ENABLED_JOBS`. A disabled job returns HTTP 200 with
`status: disabled`, so a green status code proves nothing about whether it ran.

**~2,481 lines of unreachable page code.** Eleven complete page modules
(`forecast.py`, `ap.py`, `ar.py`, `ledger.py`, `alerts.py`, `scenario.py`,
`upload_page.py`, `qbo_settings.py`, `calendar_view.py`, `reconcile.py`,
`alerts_view.py`) whose routes deliberately redirect to the control page. They
are not bugs, they are a trap: two of them were mistaken for missing features and
nearly rebuilt. Check whether a renderer is imported anywhere before assuming a
feature does not exist.

---

## 5. Rules that must hold

- Money in integer cents. Percentages in basis points.
- **Predictions are never persisted** to `cash_events`. That is what keeps stored
  history actuals-only. A prediction must never reach a backward-looking total.
- Bank and QuickBooks access is **read only**. No write-back, no moving money.
- Nothing hard-deleted without an undo path.
- Additive migrations go in **both** the Postgres and SQLite paths in
  `models/database.py`, inspector-guarded and idempotent.
- Any set of counters shown side by side **must add up to their stated total**.
  This broke twice in one day. Assert it, do not assume it.
- Every feature ships with a test in the same commit.
- David is not technical. UI copy is plain English, no jargon, no em dashes.

---

## 6. Verification discipline

Three consecutive clean passes before anything is handed over. Any finding resets
the counter.

1. **Code gate.** Full suite plus a zero-net-regression diff against a fresh
   baseline worktree at `origin/main`.
2. **Integration gate.** Clean re-read from disk, every new route exercised
   through the real router (this is where a missing import shows up and nowhere
   else), migrations applied to both a fresh and an existing database.
3. **Live walk.** Load the real page on `agent.anatainc.com` with real data. Both
   production bugs this session were invisible to a green suite and obvious on
   the live page.

**Prove every guard catches its bug.** Reintroduce the fault and watch the test
go red. Two guards written this session passed on both the broken and the fixed
code and had to be thrown away.

**Plausibility, not just structure.** A green suite proves the strings formatted.
It does not prove the rent is not four times its real value, which is exactly
what shipped before the live page was loaded.

---

## 7. Numbers as of handoff

For sanity checking that nothing has drifted:

- Spendable cash $25,215.22, reserves $287.42, 5 accounts across 2 banks
- Cash floor $10,000
- Boulder Ranch about $42,089 a month, $10,075 paid, $32,014 remaining
- Next week (Aug 10 to 16): $2,056 still due, $5,873 unconfirmed
- 63 recurring vendors detected from bank history
- ClickUp: 0 live rows

---

## 8. Open questions for David

- Does a draft payroll run count as committed cash?
- What contract fields matter for vendors (renewal date, notice period, term)?
- Should a credit card get its own account role?
- The rent plan has never been observed across a full month. It currently says
  send Boulder Ranch $15,215 today. Worth watching one real month before trusting
  it unsupervised.
