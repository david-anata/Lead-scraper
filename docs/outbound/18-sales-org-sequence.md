# Sales-org sequence

David approved this copy. It is not created in Instantly yet. The segment stays off until `OUTBOUND_SALES_ORG_ENABLED` is true and `OUTBOUND_SALES_ORG_CAMPAIGN_ID` is set.

No URLs in any step. The booking link is sent only in a reply after a prospect responds. Delays are 3 days, then 4 days. Emails 2 and 3 have a blank subject so they thread.

## Email 1

Subject: `{{companyName}} growth idea`

Delay before the next email: 3 days.

```
Hi {{firstName}},

{{personalization}}

{{brandAngle}}

At a lot of brands, Amazon sits with sales, and it's one of the quickest places to add revenue against the number. We handle the strategy and the day-to-day ads, and we keep an eye on price and the buy box so growth doesn't come at the cost of margin. You can try it free for 14 days, and plans start at $99 a month after that.

Open to a quick intro with David, or should I send a short strategy deck for {{companyName}}?

Abby
Account Specialist, Anata
```

## Email 2

Subject: blank, so it threads.

Delay before the next email: 4 days.

```
Hi {{firstName}},

Quick follow-up. If Amazon is part of how {{companyName}} hits its number, we can usually find revenue that's being left on the table, whether that's listings, ads, or price and buy box issues. It's free to try for 14 days, then plans start at $99 a month.

Would a quick intro with David help, or should I send over a short strategy deck?

Abby
Account Specialist, Anata
```

## Email 3

Subject: blank, so it threads.

```
Hi {{firstName}},

I'll leave it here. If Amazon isn't a priority for {{companyName}} right now, no worries at all. If it is, just reply "intro" or "deck" and I'll take care of the rest.

Abby
Account Specialist, Anata
```
