# 15 - Manual Clay import: step by step

For the batch `anata_clay_batch_plan_upgrade_2026-07-25.csv` (30 brands).

Based on what your Clay workspace actually looked like on 25 Jul. If a screen differs from
what is described, stop at that step and say so rather than guessing.

**The order matters.** Steps 1 to 4 cost nothing and prevent the two most likely ways this
wastes credits or looks broken. Do them before importing.

---

## Step 1 - Turn Auto-run OFF first

Open **Anata // Claude Table** and click the **Found Contacts** tab at the bottom.
Top left of the table there is an **Auto-run** toggle.

**Turn it off before you import anything.**

If Auto-run is on, the moment 30 rows land every enrichment column fires at once. You would
spend credits on all 30 before knowing whether the first one even works. We want one row
first.

## Step 2 - Confirm Sandbox Mode is OFF

Top right of the table there is a **Sandbox Mode** button. It must be **off**.

Sandbox simulates enrichment. It is useful for checking a prompt cheaply, but the results
are not real, so an email produced in sandbox cannot be sent to anyone. If you leave it on
you will get output that looks right and is worthless.

## Step 3 - Clear the filter

The Found Contacts table has a filter on it (**Email Present**), and it was showing
**0 of 10 rows** with "No matching rows found".

Clear or disable that filter before importing.

If you leave it on, your 30 new rows will have no email yet, so the filter will hide every
one of them and the import will look like it failed when it actually worked. This is the
single most likely thing to make you think something is broken.

## Step 4 - Decide about the 10 existing rows

There are 10 rows already in the table from earlier testing, and all their enrichment
columns are empty. Deleting them makes this test unambiguous: anything you see afterwards
came from this import. Keeping them is fine too, just remember they are not yours.

---

## Step 5 - Import the file

Bottom left of the workbook, click **+ Add**, then under **Sources** choose
**Import from CSV**. Upload `anata_clay_batch_plan_upgrade_2026-07-25.csv`.

Make sure it imports **into the Found Contacts table**, not into Recent Store Leads.
Found Contacts is the one that holds Sales Fit and Personalized Cold Outreach. Recent Store
Leads is the company table fed by Clay's own StoreLeads signal and is not where the prompts
live.

## Step 6 - Map the columns

The file has these ten columns. Map them like this:

| Column in the file | Where it goes | Why |
|---|---|---|
| `brand` | Merchant Name | the company name |
| `domain` | Domain | what Clay enriches from |
| `reason` | **new column, call it `personalization`** | see the warning below |
| `recipe` | new column, `recipe` | which pull sourced it, needed for measurement |
| `tier` | new column, `tier` | A / B / C quality |
| `niche` | new column, `niche` | |
| `country` | new column, `country` | |
| `score` | new column, `score` | |
| `estimated_sales_yearly_cents` | new column, `revenue_cents` | it is in cents, so $8.4M reads as 835227012 |
| `categories` | new column, `categories` | |

**The one that matters most is `reason`.** It already contains the finished sentence
"They upgraded their store plan in the last 60 days, which usually means new budget."
That is the why-now hook the email is built around.

Two ways to use it, pick one:
- **Simplest:** let it flow through as the personalization line itself.
- **Better:** feed it into your Personalized Cold Outreach prompt as context, so the AI
  writes a sharper opener grounded in that fact.

Either way, **the column name has to match what the email actually reads.** The Instantly
copy uses `{{personalization}}`, and Instantly variable names are case sensitive and must
match exactly. If it does not match, the opening line comes out blank and nothing warns you.

## Step 7 - Check the rows landed

You should see **30 new rows**, each with a brand name and a domain. If you see nothing,
go back to Step 3, it is almost certainly the filter.

---

## Step 8 - Run ONE row before you run thirty

Pick a single row and run the enrichment on just that row.

Then check, on that one row:
1. **Work Email** filled with a real, verified address
2. **Sales Fit** returned a sensible yes or no with a reason you agree with
3. **Personalized Cold Outreach** wrote something specific and true, not generic filler

If any of those three is empty or wrong, stop. Fixing a prompt now costs one credit.
Finding out after 30 rows costs thirty.

The playbook is blunt about this: bad personalization is worse than none, because it reads
as mass-produced and lowers reply rates. Read the sentence and ask whether you would send it.

## Step 9 - Run the remaining rows

Once one row looks right, run the other 29. Expect fewer than 30 usable leads at the end.
Not every brand will yield a verified email, and Sales Fit will reject some. That is the
system working, not failing.

## Step 10 - Get the qualified rows into Instantly

**This step does not exist in your table yet.** There is no send-to-Instantly action on
Found Contacts, so nothing will reach the campaign on its own.

You need a Clay action that sends rows into the Instantly campaign **Anata // Claude**, and
it should only send rows where Sales Fit passed and a verified email exists. When you add
it, the fields Instantly needs are the email address, a first name, and the personalization
line.

## Step 11 - Before any real send

1. Add one test lead with your own address in Instantly and open Preview.
2. Confirm the braces disappear and a name appears. Spintax renders in preview, so if you
   still see `{` or `|` in the delivered test, stop and fix it.
3. Read the delivered email on a phone.
4. Only then turn the campaign on.

---

## If the import fails
These 30 brands are marked as already contacted, so they will not be offered again on a
future pull. If you discard the file, say so and they can be released back into the pool
rather than being stranded.

## What to report back
The useful answers are: did all 30 rows land, did the one test row produce a real verified
email, and did the personalization sentence read like something a human would send.
