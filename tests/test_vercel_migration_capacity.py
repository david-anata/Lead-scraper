from __future__ import annotations

from scripts import vercel_migration_capacity as capacity


def test_capacity_passes_with_required_headroom() -> None:
    receipt = capacity.assess_capacity(
        source_snapshot_bytes=800,
        target_database_bytes=100,
        project_limit_bytes=2_000,
        headroom_multiplier=2.0,
    )

    assert receipt["ok"] is True
    assert receipt["required_bytes"] == 1_700
    assert receipt["available_bytes"] == 1_900


def test_capacity_fails_when_project_limit_is_too_small() -> None:
    receipt = capacity.assess_capacity(
        source_snapshot_bytes=800,
        target_database_bytes=100,
        project_limit_bytes=1_500,
        headroom_multiplier=2.0,
    )

    assert receipt["ok"] is False
    assert receipt["required_bytes"] == 1_700


def test_capacity_fails_closed_when_limit_is_unknown() -> None:
    receipt = capacity.assess_capacity(
        source_snapshot_bytes=800,
        target_database_bytes=100,
        project_limit_bytes=None,
    )

    assert receipt["ok"] is False
    assert receipt["capacity_known"] is False

