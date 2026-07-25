# Switch the Clay table's input to your app's list

Right now your cloned Clay table pulls from StoreLeads by itself. We want your
app to be the gatekeeper instead (it applies your priorities and never repeats a
brand), and hand Clay only the approved list. Same table, same prompts, we just
change where its rows come from.

## Until August 6 (CSV, by hand)
1. In your app, open the brands download and save the file. (The button lives at
   `/admin/api/outbound/brands.csv` once this is live on your app.)
2. In your Clay table, click the source at the top left (it currently says
   "Find companies with Store Leads").
3. Pause or remove that StoreLeads source so Clay stops pulling on its own.
4. Add a new source: **Import CSV**, and upload the file from step 1.
5. Clay matches the columns (domain, brand, niche) automatically.
6. Your existing columns (Work Email, Sales Fit, personalization) run on those
   imported rows exactly as they do now.

## After August 6 (automatic)
When Clay's live pipe unlocks on the Growth tier, we swap the CSV import for the
automatic feed (the app pushes new approved brands straight in) and you stop
importing by hand. The code for that is already written and waiting.

## Why this order
Your app applies your target and your do-not-repeat memory before anything
reaches Clay. That keeps one brain in charge of who gets contacted, and it feeds
your scoreboard so you can see performance in one place.
