# Personalization sample

Before and after for leads named in the Nov 9, 2026 audit.
Before is what today's builder writes from the facts still stored on the lead.
An empty before line means we did not have a category or recipe sentence, not that Instantly sent a blank.
After is the deterministic line with OUTBOUND_RICH_PERSONALIZATION_ENABLED on.
Counts are words. No line contains a digit or an em dash.
Rows marked dropped would not be emailed. The line is here so the wording can be reviewed.
abby_reengage rows stay empty until the dry-run backfill has a StoreLeads profile.
This file did not call Instantly or change any lead.

## amz_on: indrek@richhaircare.com

Brand: RICH Hair Care

Note: StoreLeads bulk lookup in the Nov 9 audit. Category was not in that lookup, so the before line is empty.

Before personalization: (empty)

Before brandAngle: (empty)

Facts the new builder would use: sells on Amazon, is sold in retail stores, uses Klaviyo, runs ads on Meta, runs ads on Google, moved from WooCommerce in September, sells fourteen products, is based in Philadelphia

After personalization: RICH Hair Care sells on Amazon, is sold in retail stores, and uses Klaviyo.

After brandAngle: Anata helps RICH Hair Care grow. The brand sells on Amazon.

## amz_off: olle@northtrampoline.com

Brand: North Trampoline

Note: Recipe filter only. The live line in the audit was a generic catalog claim, not a fact about this brand.

Before personalization: (empty)

Before brandAngle: (empty)

Facts the new builder would use: does not sell on Amazon

After personalization: North Trampoline does not sell on Amazon.

After brandAngle: Anata helps North Trampoline grow. The brand does not sell on Amazon.

## amz_off: dylan@r-pur.com

Brand: R-Pur

Note: Recipe filter only.

Before personalization: (empty)

Before brandAngle: (empty)

Facts the new builder would use: does not sell on Amazon

After personalization: R-Pur does not sell on Amazon.

After brandAngle: Anata helps R-Pur grow. The brand does not sell on Amazon.

## amz_off: marla@bbqing.com

Brand: BBQing

Note: Title E-commerce Merchandise Associate. The title gate drops this contact. Copy is shown for review only.

Before personalization: (empty)

Before brandAngle: (empty)

Facts the new builder would use: does not sell on Amazon

After personalization: BBQing does not sell on Amazon.

After brandAngle: Anata helps BBQing grow. The brand does not sell on Amazon.

## amz_off: sappho@helloryse.com

Brand: Helloryse

Note: Title Marketing & Support Associate. The title gate drops this contact. Copy is shown for review only.

Before personalization: (empty)

Before brandAngle: (empty)

Facts the new builder would use: does not sell on Amazon

After personalization: Helloryse does not sell on Amazon.

After brandAngle: Anata helps Helloryse grow. The brand does not sell on Amazon.

## amz_off: dayana@moxielash.com

Brand: MoxieLash

Note: Recipe filter only. Title was E-Commerce and Marketing Operations, which is not a buyer title.

Before personalization: (empty)

Before brandAngle: (empty)

Facts the new builder would use: does not sell on Amazon

After personalization: MoxieLash does not sell on Amazon.

After brandAngle: Anata helps MoxieLash grow. The brand does not sell on Amazon.

## plan_upgrade: jennifer@kitbetter.com

Brand: Kitbetter

Note: Recipe signal only. The audit gave both Jennifer and Grace the same plan-upgrade line.

Before personalization: Kitbetter recently upgraded their store plan.

Before brandAngle: Anata helps Kitbetter grow after a recent plan upgrade.

Facts the new builder would use: is on Shopify Plus

After personalization: Kitbetter is on Shopify Plus.

After brandAngle: Anata helps Kitbetter grow. The brand is on Shopify Plus.

## plan_upgrade: grace@kitbetter.com

Brand: Kitbetter

Note: Same brand as Jennifer. One opener per domain.

Before personalization: Kitbetter recently upgraded their store plan.

Before brandAngle: Anata helps Kitbetter grow after a recent plan upgrade.

Facts the new builder would use: is on Shopify Plus

After personalization: Kitbetter is on Shopify Plus.

After brandAngle: Anata helps Kitbetter grow. The brand is on Shopify Plus.

## plan_upgrade: cammie@rockflowerpaper.com

Brand: Rockflowerpaper

Note: Recipe signal only.

Before personalization: Rockflowerpaper recently upgraded their store plan.

Before brandAngle: Anata helps Rockflowerpaper grow after a recent plan upgrade.

Facts the new builder would use: is on Shopify Plus

After personalization: Rockflowerpaper is on Shopify Plus.

After brandAngle: Anata helps Rockflowerpaper grow. The brand is on Shopify Plus.

## replatform: jaine@aurahome.com.au

Brand: Aura Home

Note: Would be dropped. The audit found this store outside the US. Previous platform was not in the audit, so the line uses the recipe fact only.

Before personalization: Aura Home recently moved their store to a new platform.

Before brandAngle: Anata helps Aura Home grow after a recent platform move.

Facts the new builder would use: recently moved their store to a new platform

After personalization: Aura Home recently moved their store to a new platform.

After brandAngle: Anata helps Aura Home grow. The brand recently moved their store to a new platform.

## replatform: lucie@thecheesesociety.co.uk

Brand: The Cheese Society

Note: Would be dropped. UK store. Previous platform was not in the audit.

Before personalization: The Cheese Society recently moved their store to a new platform.

Before brandAngle: Anata helps The Cheese Society grow after a recent platform move.

Facts the new builder would use: recently moved their store to a new platform

After personalization: The Cheese Society recently moved their store to a new platform.

After brandAngle: Anata helps The Cheese Society grow. The brand recently moved their store to a new platform.

## replatform: sigourney@beauticate.com

Brand: Beauticate

Note: Recipe signal only. Previous platform was not in the audit.

Before personalization: Beauticate recently moved their store to a new platform.

Before brandAngle: Anata helps Beauticate grow after a recent platform move.

Facts the new builder would use: recently moved their store to a new platform

After personalization: Beauticate recently moved their store to a new platform.

After brandAngle: Anata helps Beauticate grow. The brand recently moved their store to a new platform.

## abby_reengage: sunny@sunnyskorean.com

Brand: Sunny's Korean

Note: No StoreLeads profile yet. The backfill dry-run fills this.

Before personalization: (empty)

Before brandAngle: (empty)

Facts the new builder would use: (none on file)

After personalization: (empty)

After brandAngle: (empty)

## abby_reengage: fischer@xviveaudio.com

Brand: Xvive

Note: No StoreLeads profile yet. Positive reply on Nov 5.

Before personalization: (empty)

Before brandAngle: (empty)

Facts the new builder would use: (none on file)

After personalization: (empty)

After brandAngle: (empty)

## abby_reengage: ducmai@ichooseglobal.com

Brand: iChoose Global

Note: No StoreLeads profile yet. The duplicate lead is why positives landed on the old campaign.

Before personalization: (empty)

Before brandAngle: (empty)

Facts the new builder would use: (none on file)

After personalization: (empty)

After brandAngle: (empty)

## amz_off: coziya@coziya-outdoor.com

Brand: Coziya

Note: Would be dropped. StoreLeads country is EE. Facts are from the Nov 9 bulk lookup, so the line shows what the builder would have said.

Before personalization: (empty)

Before brandAngle: (empty)

Facts the new builder would use: does not sell on Amazon, uses Klaviyo, is growing on TikTok, sells twelve products, has Trustpilot reviews, is based in Tallinn

After personalization: Coziya does not sell on Amazon, uses Klaviyo, and is growing on TikTok.

After brandAngle: Anata helps Coziya grow. The brand does not sell on Amazon.
