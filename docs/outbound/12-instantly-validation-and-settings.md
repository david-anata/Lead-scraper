# 12 - Instantly validation + the settings that make us compliant

Validated live in Instantly on 2026-07-25 by inspecting the account directly.

---

## What the live account actually shows

**Campaign "Anata // Claude"** (`56a13f93-a364-40f9-ab83-5b19a93f8eb1`)
- Status: **Draft**, 0% completed.
- Sequence: **Step 1 is empty.** `<Empty subject>` and an empty body. No copy is loaded.
- Sending accounts: **none attached.** The "Accounts to use" box is empty, so the
  campaign physically cannot send.
- Leads: none.

**Verdict: the campaign exists as a blank shell.** It is not a functioning campaign yet.

### The other two campaigns (this is our real baseline)
| Campaign | Status | Sent | Replies | Opportunities | Positive rate |
|---|---|---|---|---|---|
| AGENT SDR // MARCH '26 | Active | 2,731 | 37 | 3 | 0.11% |
| Amazon / Supplements / Shopify | Completed | 1,824 | 30 | 1 | 0.05% |
| **Total** | | **4,555** | **67** | **4** | **0.09%** |

**This independently confirms our scoreboard is reading Instantly correctly:** the app
shows 4,555 sent and 0.1% positive, which matches these campaigns exactly (2,731 + 1,824 =
4,555 sent; 3 + 1 = 4 opportunities).

It also sets the bar honestly. Our positive reply rate today is about **0.09%**, roughly
**11x below** the 1.0% target. That is the single number the whole system now optimizes for.

---

## What has to happen in Instantly before sending

1. **Attach sending accounts.** Select the warmed mailboxes under "Accounts to use".
   Nothing sends until this is done.
2. **Load the v2 copy** from `docs/outbound/11-copy-audit.md` (email 1 and email 2 only;
   hold email 3).
3. **Turn Delivery Optimization on.** Instantly's own "Recommended" block does exactly what
   the brief demands: disables open tracking and sends text-only. Turn on:
   - Disables open tracking
   - Send emails as text-only (no HTML)
   - Send first email as text-only
4. **Turn Link tracking OFF.** (Brief 5.4: tracking is a top deliverability killer.)
5. **Turn "Stop sending emails on reply" ON.** Anyone who replies must stop receiving the
   sequence.
6. **Set the daily limit** to match the mailbox count at roughly 25/day per mailbox.
7. **Confirm warmup is on** for every attached mailbox (Email Accounts screen).
8. **Preview the spintax** and run the copy through a spam-word check.

---

## Settings to add on Render so the scoreboard can prove compliance

The compliance panel never guesses. Anything it cannot see reads "Confirm" until you set
the matching value. Add these to the **sales-support-agent** service:

| Setting | Value | What it does |
|---|---|---|
| `OUTBOUND_TRACKING_DISABLED` | `true` | Confirms open/click tracking is off |
| `OUTBOUND_WARMUP_ON` | `true` | Confirms warmup is on for all mailboxes |
| `OUTBOUND_DEDICATED_DOMAINS` | `true` | Confirms we never send from anatainc.com |
| `OUTBOUND_EMAIL1_TEXT_ONLY` | `true` | Confirms email 1 has no links or opt-out |
| `OUTBOUND_SPINTAX_ON` | `true` | Confirms spintax is live and previewed |
| `OUTBOUND_VERIFIED_ONLY` | `true` | Confirms we only send to verified addresses |
| `OUTBOUND_COPY_APPROVED` | `true` | Set only after you approve the v2 copy |
| `OUTBOUND_EMAILS_PER_DAY_CAPACITY` | e.g. `300` | Real sending capacity, fills the bottleneck panel |
| `OUTBOUND_SALES_MEMBERS` | `2` | You and Gabe (this is the default already) |
| `OUTBOUND_CLAY_CREDITS_REMAINING` | your balance | Fills the Clay row of the bottleneck panel |
| `OUTBOUND_SEQUENCE_EMAILS` | `2` | We launch with two emails, per the playbook |
| `OUTBOUND_POSITIVE_REPLY_TARGET_PCT` | `1.0` | The KPI target (tune as data lands) |

Set only what is genuinely true. A setting marked `true` that is not actually true makes the
scoreboard lie to you, which is worse than an honest "Confirm".

---

## Clay, restated from the live review

- Correct table: **Anata // Claude Table -> Found Contacts**.
- Sales Fit and Personalized Cold Outreach columns exist and are correct.
- **Enrichment has never run** (Work Email 0%, Sales Fit 0%, Personalization 0%).
- **No push-to-Instantly step exists** (searched all 33 columns for "instantly" and
  "campaign" - zero results).
- Add a **Clean brand name** column before any copy merges `{{brand_clean}}`
  (Playbook 3/11: "Pacific Creative Group LLC" in an email body is a mail-merge tell).
- Confirm **Sandbox Mode is OFF** before a real run, or enrichment only simulates.
