# Personalization style

`OUTBOUND_RICH_PERSONALIZATION_ENABLED` stays off until David turns it on.
When it is on, the opener and brandAngle come from the model, using a
shopper-visible fact sheet from the StoreLeads record and the examples
below. The code then checks the lines. A thin record, a restricted
category such as wine, or a high vendor count (a multi-brand retailer)
gets no lines and is flagged. If the model line fails a check, the
fallback is a short category sentence, or nothing when that is not safe.
A line over the hard word max is not replaced. It is left empty and the lead is held.

The StoreLeads key the code reads is `STORELEADS_API_KEY`. It is not set
on the Vercel project anata-agent-staging. Do not invent another name.

## What may be used

- description and title, unless they are only the brand name. Discount codes and shipping offers are stripped.
- contact_info descriptions and follower counts (social bios).
- sales_channels and sales_channel_urls, to confirm Amazon, Etsy, or Faire. The Amazon FBA technology tag is not proof.
- store_locator_page, retailer_page, and features, for retail and wholesale.
- Review aggregates from Yotpo, Okendo, Judge.me, Loox, or Trustpilot. Cite only a high average, and never name the review app.
- cluster_domains, locs, and ships_to_countries, for international site versions.
- merchant_name.
- A subscribe option, without naming the app that provides it.

vendor_count, when high, means a multi-brand retailer and the lead is skipped.
last_platform, plan, and apps are for choosing the recipe only. They never appear in the copy.
estimated_sales, visits, rank, and employee_count never appear in the copy.

## Rules

- Opener (`{{personalization}}`): one sentence of about 20 words, never more than 25. A longer line is regenerated once, then the lead is held.
- brandAngle: one sentence of about 15 words, never more than 20. Same regenerate-once-then-hold rule. It does not repeat the opener and never starts with "Anata helps".
- Only mention Amazon when a fact says the brand sells there. A Trial Offer line never mentions Amazon and never assumes the brand sells there.
- The examples below show voice. Live lines stay inside the word caps above.
- No digits. Write numbers as words.
- No em dashes, no URLs, and nothing that is not in the fact sheet.
- No negative lead, such as "does not sell on Amazon".
- No hype, such as "perfect moment" or "maximize".
- Never cite a low rating.
- For every recipe, and especially plan_upgrade and replatform: never name platforms, plans, apps, tools, or ad pixels, and never say "moved" or "upgraded" or give dates of backend changes. Use shopper-visible facts: products, story, design details, review volume, stockists, social, international site versions, and "subscribe option".

## Approved examples

### amz_on, Lovevery

Opener: Lovevery's stage-based Play Kits are a smart way to sell toys, and with more than half a million TikTok followers, a lot of parents probably discover you there and then go looking on Amazon. That search is the moment your Amazon store either catches them or loses them to a lookalike Montessori toy.

Angle: The play-stage videos you already make for YouTube and Pinterest could double as Amazon listing video, helping parents pick the right toy for their child's age without leaving the page.

### amz_on, REV Gum

Opener: Caffeine and electrolytes in a sugar-free gum is an easy thing to explain in one line, and I noticed REV is already on convenience store shelves and gopuff alongside your Amazon store. People who try a pack at the counter often go to reorder online, and Amazon is usually the first place they check.

Angle: Leaning into the electrolyte side and the multi-pack value on Amazon could set REV apart from shoppers who are only comparing plain caffeine gums.

### amz_off, Water.io

Opener: A smart bottle that comes with free hydration coaching in an app is an easy idea to get behind, and your reviews show buyers are happy with it. Products like this, where people want to understand how it works before buying, do well on Amazon when the listing shows the app clearly.

Angle: Putting the companion app front and center in the Amazon images could set Water.io apart from the plain tracker bottles shoppers compare it against.

### plan_upgrade, Happy Viking

Opener: A plant-based shake that covers protein, fiber and probiotics in one go is a simple promise, and thousands of reviews near five stars suggest Happy Viking keeps it. That kind of loyalty usually means there are a lot more people out there who would love it if they found it.

Angle: Plenty of protein shoppers start their search on Amazon, and a subscribe option there would let new fans settle into the same daily habit your current customers already have.

### plan_upgrade, Joybees

Opener: The honeycomb footbed and Popinz charms make Joybees an easy yes for families, and it's great to see you in stores and on sites beyond the US. Shoes that are washable and easy to slip on are exactly what busy parents reach for.

Angle: Pairing Popinz charms with clogs or sandals as ready-made gift sets on Amazon could turn a single shoe purchase into a bigger, more fun first order.

### replatform, Wove Made

Opener: Sending couples a replica ring to try on at home before you build the real one is such a thoughtful idea. It takes a lot of the nerves out of one of the biggest purchases people ever make.

Angle: Ready-to-ship wedding bands or fine jewelry pieces could be Wove's entry point on Amazon, introducing couples to the brand before they book a design consultation.

### replatform, Krazy Kreations

Opener: Krazy Kreations is known among card makers for its vinyl outline stickers, and the handmade micro-fine glitter paper is a lovely detail. The crafting ideas you share on YouTube and Pinterest make it easy to see what people can make with them.

Angle: Low-priced craft supplies tend to work better on Amazon as themed or seasonal kits, which makes each order worth shipping and gives crafters a reason to come back.

### abby_reengage, JaxKelly

Opener: Shower Affirmations is such a memorable product, and thousands of reviews averaging close to five stars say your customers agree. You're already on Etsy and in wholesale, so Amazon would just be one more place where those reviews do the selling.

Angle: Shower Affirmations make an easy gift, so an Amazon setup built around gifting seasons could carry them next to the jewelry for shoppers who never come across your Etsy shop.
