# 10 - 100 Validation Questions (honest answers + evidence)

Status legend:
- ✅ Proven by an automated test or in-process integration run (this session).
- 🔎 Proven by live inspection of the real system (browser review).
- ⏳ Built + tested in code, but live-on-site only after David deploys the latest commit.
- 🔒 Cannot be done by me: needs David's credentials, approval, or a real send.
- ❓ Cannot answer yet: missing input.

Evidence tags point to the test files and the in-process integration check (Pass 2)
run this session. Full suite this session: 1,866 passed, 0 new failures (the 21
pre-existing failures are unrelated env/LLM/Gmail tests, identical with and without
these changes).

Honest headline: the whole APP half is built, tested, and pushed to `main`. It is
NOT live on agent.anatainc.com until David does one Manual Deploy. The Clay and
Instantly halves are set up by David (one step, the Instantly key, only he can do),
and nothing sends until he approves the copy and a test batch. The "compare to the
YouTube video" step cannot be done because that video was never provided.

---

## A. Brand sourcing and dedup (StoreLeads -> CSV)

1. Does the app pull brands from StoreLeads? ✅ Yes (outbound_pipeline, StoreLeads-only).
2. Is the pull rate-limit safe? ✅ Retries on 429 with backoff; paces pages; returns partial instead of failing (test_outbound_pipeline).
3. If StoreLeads cuts us off mid-run, do we keep what we gathered? ✅ Yes, `partial` flag set, brands returned (test_outbound_pipeline).
4. Does the ICP gate enforce Shopify only? ✅ test_outbound_pipeline (platform gate).
5. Does it enforce the revenue band ~$1M-$15M? ✅ Yes (min/max cents tests).
6. Does it enforce US/UK/CA/AU only? ✅ Yes, with UK normalized to GB.
7. Does it require a contact email? ✅ Yes (`_has_email`, tested).
8. Does it exclude agencies/wholesalers/dropshippers/print-on-demand? ✅ Exclude-keyword test.
9. Does it map a brand into one of the six niches? ✅ test_niche_is_detected.
10. Are off-niche brands rejected? ✅ test_off_niche_rejected.
11. Does the CSV download work on the live site? ⏳ Route returns a clean 400 without a key and streams CSV with one (Pass 2 + code); live after deploy.
12. Is the CSV importable by Clay (plain columns)? ✅ Header + flattened lists (test_csv_has_header_and_rows).
13. Never email a brand twice across downloads? ✅ Dedup memory persists domains (test_outbound_memory).
14. Does dedup survive a redeploy? ✅ Backed by the app database table, not memory.
15. If the database is down, does the CSV still build? ✅ Fails open (test_load_fails_open_on_bad_engine + code path).
16. Are exported brands recorded with their tier and signals? ✅ record_leads/load_pushed (test_outbound_memory).
17. Is domain matching case/whitespace safe? ✅ Normalized lower/strip (tests).
18. Can the download count be adjusted (1-500)? ✅ Clamped in the route; input on the page.
19. Does the pull request the richer fields the scorer needs? ✅ Fields expanded (technologies, spend, plan, etc.).
20. Is any of this able to send an email by itself? ✅ No. Sourcing is send-free by design.

## B. Signal scoring and tiers

21. Does each brand get a fit score? ✅ score_store (test_outbound_scoring).
22. Meta + Google pixels scores the ad-spend signal? ✅ test_meta_google_ads_scores_three.
23. A third ad channel adds more? ✅ test_multichannel_adds_two_more.
24. A CRO/testing app is detected and scored? ✅ test_cro_app_scores_three.
25. A recent growth-app install becomes the "why now" reason? ✅ test_recent_growth_app_install_is_the_reason.
26. An old install does NOT count as recent? ✅ test_old_install_does_not_trigger_recent.
27. A recent plan upgrade is detected? ✅ test_recent_plan_upgrade.
28. Healthy app spend is rewarded? ✅ test_healthy_app_spend.
29. A trending tag is rewarded? ✅ test_trending_tag.
30. No ad pixel is penalized? ✅ test_no_ad_pixel_penalized_and_tier_c.
31. Public companies are excluded (Tier X)? ✅ test_public_company_excluded.
32. Enterprise analytics stacks are penalized? ✅ test_enterprise_stack_penalized.
33. Tier A boundary (8+) is correct? ✅ test_tier_a_at_eight.
34. Tier B range is correct? ✅ test_tier_b_range.
35. Do public companies get dropped from the CSV? ✅ Runner skips Tier X.
36. Is the CSV sorted hottest-first? ✅ Runner sorts by score desc.
37. Does each lead carry tier + reason for the opener? ✅ test_to_clay_lead_carries_tier_and_reason.
38. Does a malformed date crash scoring? ✅ No (test_bad_date_does_not_crash).
39. Is the reason a human sentence (personalization seed)? ✅ Yes (plain-English labels).
40. Are the weights the ones in the strategy doc (08)? ✅ Yes, mirrored.

## C. Capacity and bottlenecks

41. Does the scoreboard show a capacity panel? ⏳ Rendered in the page body (Pass 2 marker present); live after deploy.
42. Is under-capacity flagged? ✅ test_under_capacity_is_flagged.
43. Is the single biggest jam named correctly? ✅ test_biggest_is_worst_ratio.
44. When all capacity is fine, does it say so? ✅ test_no_bottleneck_when_all_ok.
45. Do unknown numbers avoid falsely winning "biggest jam"? ✅ test_unknown_never_wins_biggest.
46. Zero capacity but real need = infinite shortfall? ✅ test_zero_have_but_need_is_infinite_shortfall.
47. Does it ask for numbers when they are missing? ✅ test_headline_asks_for_numbers_when_missing.
48. Emails/day need derived from the booked-call goal? ✅ test_defaults_give_sensible_email_need (6000/day at defaults).
49. Live emails-per-booked-call overrides the assumption? ✅ test_live_epc_overrides_assumption.
50. Sales members default to 2 (David + Gabe)? ✅ test_members_default_to_two.
51. Are capacity inputs configurable without code? ✅ Yes, via env (documented in the module).
52. Does the panel render its rows + headline? ✅ test_render_contains_rows_and_headline.

## D. Per-signal efficacy

53. Are counts shown per signal before outcomes exist? ✅ test_counts_sent_per_signal_without_outcomes.
54. Are positive rate and lift computed when outcomes exist? ✅ test_positive_rate_and_lift_with_outcomes.
55. Is the table sorted by volume? ✅ test_sorted_by_sent_desc.
56. Empty state handled? ✅ test_empty_pushed + test_empty_state.
57. Does it honestly say rates are waiting on replies? ✅ test_counts_render_with_waiting_note.
58. Does efficacy read the pushed leads with signals? ✅ load_pushed feeds it (memory + router).
59. Is the baseline only set once real outcomes exist? ✅ baseline None without outcomes (test).
60. Is per-lead outcome data connected to Instantly yet? 🔒 No. Instantly's aggregate endpoint has no per-lead outcomes; this needs a per-reply feed. Panel is honest until then.

## E. HubSpot nurture (follow-up / no-show)

61. Can a follow-up contact be enrolled? ✅ test_creates_when_no_existing_contact.
62. Is an existing contact updated instead of duplicated? ✅ test_updates_when_contact_exists.
63. Are bad outcomes rejected? ✅ test_rejects_bad_outcome.
64. Are bad emails rejected? ✅ test_rejects_bad_email.
65. If HubSpot is not connected, is it handled cleanly? ✅ test_not_connected + Pass 2 (400 with reason).
66. Are HubSpot API errors caught (no 500)? ✅ test_api_error_is_caught.
67. Does it work with the real client's property-style is_configured? ✅ test_supports_property_style_is_configured (bug found + fixed in Pass 2).
68. Does the scoreboard show the nurture form? ⏳ Present in page body (Pass 2 marker); live after deploy.
69. Is the nurture endpoint access-gated? ✅ Under /admin/api/outbound (middleware tool gate).
70. Does the nurture actually enroll into a sequence live? 🔒 Needs David to create the HubSpot property + workflow + sequence; code only stamps the trigger.

## F. App shell, navigation, access

71. Is there an Outbound tab in the app nav? ✅ Nav renders it (tested earlier this session).
72. Does it show Scoreboard + Brand List pills? ✅ Both links render.
73. Do the pages use the app shell (not bare)? ✅ _shell_page wraps them (Pass 2: 200 + shell).
74. Is access controlled by a permission? ✅ outbound.scoreboard tool gates the routes.
75. Is the tab hidden from users without that permission? ✅ Verified (nav filter test earlier).
76. Does the middleware resolve all outbound routes to the tool? ✅ Verified earlier this session.
77. Does the super-admin (David) see it? ✅ Superadmin bypass.
78. Scoreboard renders with no Instantly key (not connected state)? ✅ Pass 2 (200, panels present).
79. Brand List renders with no StoreLeads key (shows the set-key note)? ✅ Pass 2 (200, note present).
80. Are the pages live on agent.anatainc.com right now with these panels? ⏳ No, pending the Manual Deploy of the latest commit.

## G. Clay (live review this session)

81. Did I actually open and inspect Clay? 🔎 Yes, the real workspace.
82. Is the correct table identified? 🔎 Yes: "Anata // Claude Table" -> Found Contacts.
83. Do the Sales Fit and Personalization columns exist? 🔎 Yes, both present.
84. Has the enrichment ever actually run? 🔎 No: Work Email 0%, Sales Fit 0%, Personalization 0%, "0% of table completed."
85. Is there a push-to-Instantly step in Clay? 🔎 No: searched all 33 columns for "instantly" and "campaign", zero results.
86. Is Clay fed by our CSV yet? 🔎 No: it is wired to Clay's own StoreLeads signal ("No data yet").
87. Could Clay be in Sandbox Mode (simulating)? 🔎 A Sandbox toggle exists; must be confirmed off before a real run.
88. Can I run Clay enrichment to prove it? 🔒 Needs David's go-ahead (spends Clay credits).
89. Can I add the push-to-Instantly column? 🔒 Only after the Instantly key is connected (David's step) and a campaign exists.
90. Is Clay validated as send-ready? No. It has the columns but has never run and does not push anywhere.

## H. Instantly

91. Is the scoreboard reading real Instantly data? ✅ Yes (4,555 sent / 0.4% reply confirmed earlier).
92. Is there a campaign for this system yet? 🔒 No. David said one must be created.
93. Are warmed domains ready? Per David, yes (not independently verified by me).
94. Is Instantly connected to Clay (API key)? 🔒 No, and only David can enter that key.
95. Has any test send been done? 🔒 No.

## I. End-to-end send path

96. Has one brand flowed StoreLeads -> Clay -> Instantly end to end? 🔒 No, blocked on Clay run + Instantly connect (David's steps).
97. Has an internal test email been received and checked? 🔒 No, pending the campaign + a test batch.
98. Has David approved the copy? 🔒 No. Required before any real send.
99. Is anything able to send to a real brand right now? ✅ No, by design (all send steps gated).

## J. Verification integrity and the video

100. Was this validated against the YouTube video's transcript goals, start to finish? ❓ No. No YouTube video or transcript was ever provided in this conversation or memory, so I cannot compare against goals I have never seen. Everything above is validated against the approved spec (docs/outbound/09) and strategy (docs/outbound/08). If you paste the video link or transcript, I will redo this comparison against it honestly.

---

### What is fully done and proven (autonomous)
Sourcing + dedup, signal scoring + tiers, capacity/bottleneck panel, per-signal
efficacy scaffold, HubSpot nurture code, app shell + nav + access. All unit-tested
and integration-tested in-process; pushed to `main`.

### What only David can complete (the loop cannot)
1. One Manual Deploy of the latest commit to make the new panels live.
2. Enter the Instantly API key into Clay (credential; I never handle keys).
3. Create the Instantly campaign (draft).
4. Approve the email copy and a test batch.
5. Run Clay enrichment (spends credits) and add the push-to-Instantly column.
6. Provide the YouTube video/transcript for the goal comparison.

Until 1-6 are done, a real StoreLeads-to-Instantly send is not possible, and that is
correct: the machine is built and safe, waiting on the human-owned steps.
