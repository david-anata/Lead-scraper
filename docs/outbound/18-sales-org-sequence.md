# Sales-org sequence (draft)

Not created in Instantly. David approves this copy before anyone makes the campaign.

The segment is off unless `OUTBOUND_SALES_ORG_ENABLED` is true. The campaign id stays empty in `OUTBOUND_SALES_ORG_CAMPAIGN_ID` until that campaign exists. Leads route only there. A person already in another campaign is skipped, not copied.

Same offer as the other v2 recipes: free 14-day trial, then plans from $99 a month. One link, on the first email only. Signed Abby, Account Specialist, Anata. `{{personalization}}` and `{{brandAngle}}` are the current variables. The opener itself still cannot contain a digit.

Structure matches the re-engage v2 sequence: three emails, the first subject is spintax, the next two leave the subject blank so they thread.

## Email 1

Subject: `{{RANDOM | amazon and the number | quick note | {{companyName}} on Amazon}}`

Delay before the next email: 3 days.

```
{{RANDOM | Hi | Hey}} {{firstName}},

{{personalization}}

{{brandAngle}}

Amazon is one channel a sales team can use to hit the number. We build the strategy and run the ad actions, with an eye on incremental revenue, price, and the buy box. You can try it free for 14 days. After that, plans start at $99 a month.

{{RANDOM | Open to a quick intro with David | Want me to send a short strategy deck}}?

https://calendar.app.google/5TwjbbpmKaGAQuBk8

Abby
Account Specialist, Anata
```

## Email 2

Subject: blank, so it threads.

Delay before the next email: 4 days.

```
{{firstName}}, short version: free for 14 days, then plans from $99 a month. The work is incremental revenue, holding price, and protecting the buy box.

{{RANDOM | Worth a quick intro with David | Should I send the short deck}}?

Abby
Account Specialist, Anata
```

## Email 3

Subject: blank, so it threads.

```
Last one from me. If Amazon is not how {{companyName}} hits the number right now, no worries. If it is, reply and I will set up the intro or send the deck.

Abby
Account Specialist, Anata
```
