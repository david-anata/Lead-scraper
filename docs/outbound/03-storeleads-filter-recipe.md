# StoreLeads Filter Recipe

This is the exact target, translated from the ICP already coded in the app (`main.py`), into
StoreLeads' own filters. Apply these, then export to CSV or point the webhook at the Clay
table (doc 02).

## Filters to set
- **Status:** Active
- **Platform:** Shopify
- **Estimated sales:** roughly **$1M to $15M per year** (skip tiny stores, cap below enterprise)
- **Region / country:** United States, United Kingdom, Canada, Australia
- **Categories:** Beauty & Wellness (incl. supplements), Food & Beverage,
  Apparel & Accessories, Home & Lifestyle, Pets, and Baby / Kids / Toys / Gifts
- **Social / contact info:** has an email listed, and has a LinkedIn page
- **Domain type:** custom domain only (skip the `.myshopify.com` subdomains)
- **Tags:** EXCLUDE `dropshipping` and `print on demand`

## Also exclude (matches the app's do-not-contact list)
Agencies, wholesalers, distributors, generic B2B, manufacturers. StoreLeads' category filter
handles most of this; the rest gets caught by Clay's qualification step (doc 02, Prompt A).

## Start broad, then narrow
Per the plan, run all six categories at first, watch which niche replies best in the Instantly
scoreboard, then pour volume into the winner and pause the laggards.

## Export options
- **CSV:** download the filtered list and import into Clay.
- **Webhook (evergreen):** save the filter, then add a webhook that pushes each NEW matching
  store straight into the Clay table. This is how you make it self-feeding later.

## Cost note
StoreLeads' unlimited plan is about $250/month. To test the flow cheaply first, you can buy the
same filtered export from a freelancer on Fiverr for around $50. Either works to prove it out.
