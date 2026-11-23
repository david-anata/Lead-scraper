"""Shared fixtures. Catalog reads stay offline unless a test opts in."""

from __future__ import annotations

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_catalog: call fetch_public_catalog instead of the offline stub",
    )

import outbound_pipeline as pipeline

_LIVE_CATALOG = pipeline.fetch_public_catalog


@pytest.fixture(autouse=True)
def _offline_catalog(monkeypatch, request):
    """Do not call Shopify while the rest of the suite is running.

    A test marked ``real_catalog`` exercises ``fetch_public_catalog`` itself
    and supplies its own HTTP stub.
    """

    if request.node.get_closest_marker("real_catalog"):
        monkeypatch.setattr(pipeline, "fetch_public_catalog", _LIVE_CATALOG)
        return
    monkeypatch.setattr(pipeline, "fetch_public_catalog", lambda *_args, **_kwargs: None)
