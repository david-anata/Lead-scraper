# Personalization sample

Before is the current builder with OUTBOUND_RICH_PERSONALIZATION_ENABLED off.
After is the new builder with that flag forced on for the row only. The flag stays off in the environment.
In Instantly now is the opener and brandAngle already stored on the pushed contact. It is not rewritten here.

This pass did not call StoreLeads or Instantly, and it did not change any lead.
STORELEADS_API_KEY is not in the Vercel project env for anata-agent-staging, and it is not set in this environment.
The Instantly key is on that project, but the decrypted value was not returned, so current campaign membership could not be re-listed.
Rows are contacts our database still has as pushed to the five current campaign ids, with no skip reason.
Country is US where that field is stored. Non-US hosts were left out.
Facts come from the StoreLeads payload already saved on the pull: category, app names, country, and signal text the pull recorded.
amz_on and amz_off rows below have category and country only. No retail page, ad pixel, or review app was stored, so the after line is the category fallback. It does not say the brand is off Amazon.
abby_reengage has no rows in outbound_enriched_contacts. Two US brands that replied inside that campaign still have a stored pull. The other replies in that campaign have no stored profile, so no line was written for them.

No line below was given a digit, an em dash, or a URL.

## amz_on

### lovevery.com

Brand: Lovevery

Facts used: nursery and playroom. Stored category also includes education and toys. No app list was stored for this domain. Amazon confidence on the contact is blank, so the line does not say they sell on Amazon.

In Instantly now: Lovevery sells jobs & education.

In Instantly now brandAngle: Anata helps jobs & education brands grow.

Before personalization: Lovevery sells jobs & education.

Before brandAngle: Anata helps jobs & education brands grow.

After personalization: Saw Lovevery in nursery and playroom. That is a category where Amazon demand tends to sit next to the website.

After brandAngle: Nursery and playroom shoppers compare fast, so the listing has to be ready when they look.

### chewrevgum.com

Brand: REV Gum

Facts used: food and drink. No app list stored. Amazon confidence blank.

In Instantly now: REV Gum sells food & drink.

In Instantly now brandAngle: Anata helps food & drink brands like REV Gum understand consumer demand and optimize product positioning for sustainable growth.

Before personalization: REV Gum sells food & drink.

Before brandAngle: Anata helps food & drink brands grow.

After personalization: Saw REV Gum in food and drink. That is a category where Amazon demand tends to sit next to the website.

After brandAngle: Food and drink shoppers compare fast, so the listing has to be ready when they look.

### hazelicious.com

Brand: Hazelicious

Facts used: candy and sweets. No app list stored. Amazon confidence blank.

In Instantly now: Your candy and sweets brand has room to scale beyond current distribution channels.

In Instantly now brandAngle: (empty)

Before personalization: Hazelicious sells food & drink.

Before brandAngle: Anata helps food & drink brands grow.

After personalization: Saw Hazelicious in candy and sweets. That is a category where Amazon demand tends to sit next to the website.

After brandAngle: Candy and sweets shoppers compare fast, so the listing has to be ready when they look.

### pureandcimple.com

Brand: Pure & Cimple

Facts used: skin and nail care. A second stored category is vitamins and supplements. No app list stored. Amazon confidence blank.

In Instantly now: Your skincare and nail care line has room to expand reach across new retail channels that reward direct-to-consumer brands.

In Instantly now brandAngle: (empty)

Before personalization: Pure & Cimple sells beauty & fitness.

Before brandAngle: Anata helps beauty & fitness brands grow.

After personalization: Saw Pure & Cimple in skin and nail care. That is a category where Amazon demand tends to sit next to the website.

After brandAngle: Shoppers in that category compare fast, so the listing has to be ready when they look.

## amz_off

These four were pushed on the apex domain. The US country flag is stored on the matching US host (usa. or us.). No StoreLeads app, ad, or retail field was saved for them, so the hook is the category, framed as Amazon demand next to the site.

### rivieras.com

Brand: Rivieras USA

Facts used: footwear. US host on file: usa.rivieras.com.

In Instantly now: Your apparel brand's direct-to-consumer channel deserves the same strategic attention as your wholesale partnerships.

In Instantly now brandAngle: Apparel brands grow by building community around their values, and Anata helps you reach customers who share yours.

Before personalization: Rivieras USA sells apparel.

Before brandAngle: Anata helps apparel brands grow.

After personalization: Saw Rivieras USA in footwear. That is a category where Amazon demand tends to sit next to the website.

After brandAngle: Footwear shoppers compare fast, so the listing has to be ready when they look.

### glo910.com

Brand: GLO USA

Facts used: face and body care. US host on file: usa.glo910.com.

In Instantly now: GLO USA LLC sells beauty & fitness.

In Instantly now brandAngle: Anata helps beauty & fitness brands grow.

Before personalization: GLO USA sells beauty & fitness.

Before brandAngle: Anata helps beauty & fitness brands grow.

After personalization: Saw GLO USA in face and body care. That is a category where Amazon demand tends to sit next to the website.

After brandAngle: Shoppers in that category compare fast, so the listing has to be ready when they look.

### water.io

Brand: Water.io

Facts used: beverages. US host on file: us.water.io.

In Instantly now: Water.io sells food & drink.

In Instantly now brandAngle: Anata helps food & drink brands grow.

Before personalization: Water.io sells food & drink.

Before brandAngle: Anata helps food & drink brands grow.

After personalization: Saw Water.io in beverages. That is a category where Amazon demand tends to sit next to the website.

After brandAngle: Beverages shoppers compare fast, so the listing has to be ready when they look.

### bbqing.com

Brand: BBQing.com USA

Facts used: yard and patio. US host on file: usa.bbqing.com.

In Instantly now: Your yard and patio category is competitive, and most brands miss optimization opportunities across their listings and ad spend.

In Instantly now brandAngle: (empty)

Before personalization: BBQing.com USA sells home & garden.

Before brandAngle: Anata helps home & garden brands grow.

After personalization: Saw BBQing.com USA in yard and patio. That is a category where Amazon demand tends to sit next to the website.

After brandAngle: Yard and patio shoppers compare fast, so the listing has to be ready when they look.

## plan_upgrade

App names are the ones stored on the pull. A plan upgrade is used in the angle when the opener already used the review app.

### benibeca.com

Brand: Benibeca

Facts used: Judge.me, swimwear. Judge.me is installed. That is not proof of reviews, so the line does not say shoppers have already reviewed the products. Klaviyo is also installed. The angle uses the plan upgrade.

In Instantly now: Benibeca recently upgraded their store plan.

In Instantly now brandAngle: Benibeca's store upgrade signals readiness to scale apparel sales, and Anata can accelerate that growth through data-driven strategies.

Before personalization: Benibeca recently upgraded their store plan.

Before brandAngle: Anata helps Benibeca grow after a recent plan upgrade.

After personalization: Saw Benibeca running Judge.me in swimwear, so the store is set up to collect what shoppers say.

After brandAngle: A plan upgrade is a build moment, and the marketplace listing has to keep up with the site.

### drinkhappyviking.com

Brand: Happy Viking

Facts used: Okendo, vitamins and supplements. Okendo is installed, not a count of reviews. Recharge subscriptions and Klaviyo are also installed. The angle uses Recharge.

In Instantly now: Your recent store plan upgrade suggests you're ready to scale your health and nutrition brand beyond your current reach.

In Instantly now brandAngle: (empty)

Before personalization: Happy Viking recently upgraded their store plan.

Before brandAngle: Anata helps Happy Viking grow after a recent plan upgrade.

After personalization: Saw Happy Viking running Okendo in vitamins and supplements, so the store is set up to collect what shoppers say.

After brandAngle: Subscriptions only hold if the listing price stays steady for the next order.

### joybees.com

Brand: Joybees

Facts used: Stamped, footwear. Stamped is installed, not a count of reviews. Klaviyo is also installed. A store-locator app is installed too. That app name is not treated as proof of retail stores.

In Instantly now: Your recent store plan upgrade suggests you're scaling apparel operations and could benefit from smarter inventory and demand planning.

In Instantly now brandAngle: Joybees' store upgrade shows readiness to scale, and Anata accelerates apparel growth through data-driven strategies.

Before personalization: Joybees recently upgraded their store plan.

Before brandAngle: Anata helps Joybees grow after a recent plan upgrade.

After personalization: Saw Joybees running Stamped in footwear, so the store is set up to collect what shoppers say.

After brandAngle: A plan upgrade is a build moment, and the marketplace listing has to keep up with the site.

### casekoo.com

Brand: CASEKOO

Facts used: Judge.me, mobile and wireless accessories. Judge.me is installed, not a count of reviews. Klaviyo is also installed.

In Instantly now: Your store plan upgrade suggests you're scaling operations, and we help consumer electronics brands optimize their direct-to-consumer performance.

In Instantly now brandAngle: Anata helps CASEKOO grow after a recent plan upgrade.

Before personalization: CASEKOO recently upgraded their store plan.

Before brandAngle: Anata helps CASEKOO grow after a recent plan upgrade.

After personalization: Saw CASEKOO running Judge.me in mobile and wireless accessories, so the store is set up to collect what shoppers say.

After brandAngle: A plan upgrade is a build moment, and the marketplace listing has to keep up with the site.

## replatform

### barbellvoodoo.com

Brand: Barbell VooDoo

Facts used: Recharge subscriptions, Klaviyo. Stored signal: they moved platforms recently. A separate signal says ad or affiliate apps are installed. It does not name a network, so the line does not say Meta or Google.

In Instantly now: Your apparel store's platform migration is the perfect moment to optimize how you're reaching customers across channels.

In Instantly now brandAngle: Barbell VooDoo's platform move is the perfect moment to optimize your apparel storefront for maximum conversions.

Before personalization: Barbell VooDoo recently moved their store to a new platform.

Before brandAngle: Anata helps Barbell VooDoo grow after a recent platform move.

After personalization: Saw Barbell VooDoo running Recharge subscriptions and Klaviyo, so repeat purchase and email demand are already in place.

After brandAngle: A platform move is when the Amazon listing has to be rebuilt with the new site, not left behind.

### wovemade.com

Brand: Wove Made

Facts used: Meta and Google ads, arts and crafts. The ad fact is the stored signal "Runs Meta and Google ads", not a guessed pixel. Stored signal also says they moved platforms recently. Wedding is a second category and was not used.

In Instantly now: Wove Made recently moved their store to a new platform.

In Instantly now brandAngle: Anata helps Wove Made grow after a recent platform move.

Before personalization: Wove Made recently moved their store to a new platform.

Before brandAngle: Anata helps Wove Made grow after a recent platform move.

After personalization: Saw Wove Made running Meta and Google ads in arts and crafts, so paid demand is already there to meet on Amazon.

After brandAngle: A platform move is when the Amazon listing has to be rebuilt with the new site, not left behind.

### krazykreations.us

Brand: Krazy Kreations

Facts used: platform move, arts and crafts. Stored signal includes "No ad pixel found". That negative is not the hook.

In Instantly now: Krazy Kreations recently moved their store to a new platform.

In Instantly now brandAngle: Anata helps Krazy Kreations grow after a recent platform move.

Before personalization: Krazy Kreations recently moved their store to a new platform.

Before brandAngle: Anata helps Krazy Kreations grow after a recent platform move.

After personalization: Saw Krazy Kreations moving the store onto a new platform in arts and crafts, so listings get rebuilt and Amazon gets a fresh look.

After brandAngle: The listing has to match the new site once that move is done.

### repriswines.com

Brand: Repris Wines

Facts used: platform move, alcoholic beverages.

In Instantly now: Repris Wines recently moved their store to a new platform.

In Instantly now brandAngle: Anata helps Repris Wines grow after a recent platform move.

Before personalization: Repris Wines recently moved their store to a new platform.

Before brandAngle: Anata helps Repris Wines grow after a recent platform move.

After personalization: Saw Repris Wines moving the store onto a new platform in alcoholic beverages, so listings get rebuilt and Amazon gets a fresh look.

After brandAngle: The listing has to match the new site once that move is done.

## abby_reengage

Only two of the people who have replied in this campaign have a stored US StoreLeads pull. The others have no profile on file, so they are not given a line.

### jaxkelly.com

Brand: JaxKelly

Facts used: Yotpo, clothing accessories. Yotpo is installed, not a count of reviews. Klaviyo and Faire are also installed. Faire is not treated as retail stores.

In Instantly now: How are you managing inventory across your accessory SKUs on Amazon right now?

In Instantly now brandAngle: (empty)

Before personalization: JaxKelly sells apparel.

Before brandAngle: Anata helps apparel brands grow.

After personalization: Saw JaxKelly running Yotpo in clothing accessories, so the store is set up to collect what shoppers say.

After brandAngle: Clothing accessories shoppers compare fast, so the listing has to be ready when they look.

### matatraders.com

Brand: Mata Traders

Facts used: Judge.me, apparel. Judge.me is installed, not a count of reviews. Klaviyo is also installed.

In Instantly now: Your collection of handcrafted home goods and apparel has such a distinct point of view - are you currently running Amazon Ads, or is that still untapped territory for you?

In Instantly now brandAngle: (empty)

Before personalization: Mata Traders sells apparel.

Before brandAngle: Anata helps apparel brands grow.

After personalization: Saw Mata Traders running Judge.me in apparel, so the store is set up to collect what shoppers say.

After brandAngle: Apparel shoppers compare fast, so the listing has to be ready when they look.
