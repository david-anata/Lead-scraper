"""Baseline pull splits the revenue band so it reaches past StoreLeads' 550-row window."""

from datetime import datetime

import outbound_pipeline as op
import outbound_recipes as rx
from sales_support_agent.api import outbound_jobs as jobs

BAND = {"icp.revenue_min_usd": 500_000, "icp.revenue_max_usd": 5_000_000}


def test_slices_cover_band_without_gaps():
    sl = rx.revenue_slices(BAND)
    assert len(sl) == rx.BASELINE_SLICES
    assert sl[0][0] == 500_000 * 100 // 12 and sl[-1][1] == 5_000_000 * 100 // 12
    assert all(a[1] == b[0] for a, b in zip(sl, sl[1:]))


def test_rotation_changes_daily_and_covers_all():
    d1 = rx.baseline_slices_for(datetime(2026, 10, 27), BAND)
    d2 = rx.baseline_slices_for(datetime(2026, 10, 28), BAND)
    assert d1[0] != d2[0]
    assert sorted(x["f:ermin"] for x in d1) == sorted(x["f:ermin"] for x in d2)


def test_pull_walks_slices_until_cap_and_dedupes():
    calls = []

    def fake_run(*, api_key, clay_webhook_url, processed_domains, max_new, dry_run, recipe, settings):
        p = recipe.params(datetime(2026, 10, 27), settings)
        calls.append(p["f:ermin"])
        r = op.PipelineResult(dry_run=True)
        # each slice yields 30 new brands plus one domain shared with the previous slice
        base = len(calls) * 100
        for i in range(30):
            dom = f"s{base + i}.com"
            if dom in processed_domains:
                continue
            r.leads.append({"domain": dom, "score": i})
            r.fresh += 1
        r.leads.append({"domain": "shared.com", "score": 0})
        r.fresh += 1
        r.scanned += 550
        return r

    class FakeOp:
        PipelineResult = op.PipelineResult
        run_storeleads_to_clay = staticmethod(fake_run)

    import time
    recipe = rx.recipe("icp_baseline")
    out = jobs._pull_baseline_slices(FakeOp, rx, "k", set(), 80, recipe, BAND,
                                     datetime(2026, 10, 27), time.monotonic())
    assert out.fresh >= 80 and len(calls) == 3
    assert len({l["domain"] for l in out.leads}) == len(out.leads)
    assert out.scanned == 1650


def test_caps_raised():
    assert rx.DEFAULT_SETTINGS["icp_baseline.max_per_run"] == 200
    assert jobs._DAILY_COMPANY_CAP == 300
