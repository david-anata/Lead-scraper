"""Tests for the signal scorer (docs/outbound/08). Pure functions, fixed clock."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

import outbound_pipeline as op

NOW = datetime(2026, 7, 24, tzinfo=timezone.utc)


def _tech(*names):
    return [{"name": n} for n in names]


def _app(name="", token="", categories=(), installed_at=None):
    a = {"name": name, "token": token, "categories": list(categories)}
    if installed_at is not None:
        a["installed_at"] = installed_at
    return a


def _store(**kw):
    base = {
        "name": "brand.com",
        "merchant_name": "Brand",
        "platform": "shopify",
        "country_code": "US",
        "categories": "/Beauty & Fitness/Face & Body Care",
        "estimated_sales_yearly": 3_000_000_00,
        "contact_info": [{"type": "email", "value": "hi@brand.com"}],
    }
    base.update(kw)
    return base


class ScoreSignalsTests(unittest.TestCase):
    def test_meta_google_ads_scores_three(self):
        s = _store(technologies=_tech("Facebook Pixel", "Google Ads Pixel"))
        r = op.score_store(s, now=NOW)
        self.assertIn("Runs Meta and Google ads", r["signals"])
        # +3 pixels, no CRO, no anti (pixels>0) => 3
        self.assertEqual(r["score"], 3)

    def test_multichannel_adds_two_more(self):
        s = _store(technologies=_tech("Facebook Pixel", "Google Ads Pixel", "TikTok Pixel"))
        r = op.score_store(s, now=NOW)
        self.assertEqual(r["score"], 5)  # 3 + 2

    def test_cro_app_scores_three(self):
        s = _store(technologies=_tech("Facebook Pixel", "Google Ads Pixel"),
                   apps=[_app(name="Intelligems: A/B Testing", categories=["pricing optimization"])])
        r = op.score_store(s, now=NOW)
        self.assertIn("Already uses conversion or testing tools", r["signals"])
        self.assertGreaterEqual(r["score"], 6)

    def test_recent_growth_app_install_is_the_reason(self):
        s = _store(
            technologies=_tech("Facebook Pixel", "Google Ads Pixel"),
            apps=[_app(name="Intelligems", categories=["pricing optimization"],
                       installed_at=(NOW - timedelta(days=20)).isoformat())],
        )
        r = op.score_store(s, now=NOW)
        self.assertEqual(r["reason"], "Added a growth or CRO app in the last 45 days")
        self.assertEqual(r["tier"], "A")  # 3 recent + 3 pixels + 3 cro = 9

    def test_old_install_does_not_trigger_recent(self):
        s = _store(apps=[_app(name="Intelligems", categories=["pricing optimization"],
                              installed_at=(NOW - timedelta(days=200)).isoformat())],
                   technologies=_tech("Facebook Pixel", "Google Ads Pixel"))
        r = op.score_store(s, now=NOW)
        self.assertNotIn("Added a growth or CRO app in the last 45 days", r["signals"])

    def test_recent_plan_upgrade(self):
        s = _store(technologies=_tech("Facebook Pixel", "Google Ads Pixel"),
                   plan="Shopify Plus", last_plan_change_at=(NOW - timedelta(days=30)).isoformat())
        r = op.score_store(s, now=NOW)
        self.assertIn("Upgraded their store plan recently", r["signals"])

    def test_healthy_app_spend(self):
        s = _store(technologies=_tech("Facebook Pixel", "Google Ads Pixel"), monthly_app_spend=1500_00)
        r = op.score_store(s, now=NOW)
        self.assertIn("Invests in a healthy growth app stack", r["signals"])

    def test_trending_tag(self):
        s = _store(technologies=_tech("Facebook Pixel", "Google Ads Pixel"),
                   tags=["Trending on TikTok"])
        r = op.score_store(s, now=NOW)
        self.assertIn("Trending on social right now", r["signals"])


class AntiSignalTests(unittest.TestCase):
    def test_no_ad_pixel_penalized_and_tier_c(self):
        s = _store()  # no technologies at all
        r = op.score_store(s, now=NOW)
        self.assertIn("No ad pixel found", r["signals"])
        self.assertEqual(r["tier"], "C")
        self.assertEqual(r["reason"], "Fits our ICP, thin buying signals")

    def test_public_company_excluded(self):
        s = _store(features=["Public Company"], technologies=_tech("Facebook Pixel", "Google Ads Pixel"))
        r = op.score_store(s, now=NOW)
        self.assertTrue(r["excluded"])
        self.assertEqual(r["tier"], "X")

    def test_enterprise_stack_penalized(self):
        s = _store(technologies=_tech("Facebook Pixel", "Google Ads Pixel", "Monetate"))
        r = op.score_store(s, now=NOW)
        self.assertIn("Runs an enterprise analytics stack", r["signals"])


class TierBoundaryTests(unittest.TestCase):
    def test_tier_a_at_eight(self):
        # 3 recent + 3 pixels + 2 ads-app = 8
        s = _store(
            technologies=_tech("Facebook Pixel", "Google Ads Pixel"),
            apps=[_app(name="Criteo GO", categories=["ads"],
                       installed_at=(NOW - timedelta(days=10)).isoformat())],
        )
        r = op.score_store(s, now=NOW)
        self.assertEqual(r["score"], 8)
        self.assertEqual(r["tier"], "A")

    def test_tier_b_range(self):
        s = _store(technologies=_tech("Facebook Pixel", "Google Ads Pixel"),
                   apps=[_app(name="Criteo GO", categories=["ads"])])  # 3 + 2 = 5
        r = op.score_store(s, now=NOW)
        self.assertEqual(r["tier"], "B")


class LeadIntegrationTests(unittest.TestCase):
    def test_to_clay_lead_carries_tier_and_reason(self):
        s = _store(technologies=_tech("Facebook Pixel", "Google Ads Pixel"))
        lead = op.to_clay_lead(s, now=NOW)
        self.assertIn("tier", lead)
        self.assertIn("reason", lead)
        self.assertIn("score", lead)
        self.assertEqual(lead["reason"], "Runs Meta and Google ads")

    def test_bad_date_does_not_crash(self):
        s = _store(apps=[_app(name="X", categories=["ads"], installed_at="not-a-date")])
        r = op.score_store(s, now=NOW)  # must not raise
        self.assertIsInstance(r["score"], int)


if __name__ == "__main__":
    unittest.main()
