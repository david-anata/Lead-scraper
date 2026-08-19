"""The scheduled sales operator review must not hold a database connection
across its calls to other people's services.

Every hourly run failed in production with "SSL connection has been closed
unexpectedly". The job opened one session, then spent minutes inside HubSpot,
the writeback and the operator snapshot before using that same session again.
The pool checks a connection is alive when it hands one out, not while it is
held, so nothing noticed until the dead connection was used.

These tests hold the shape that fixes it rather than the symptom: at the moment
each long external call is made, the job must be holding no connection.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.jobs.sales_operator_review import SalesOperatorReviewJob
from sales_support_agent.models.entities import Base


@pytest.fixture()
def session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


def _run(session_factory, held: dict):
    """Run the job, recording whether a connection was held at each external call."""

    def watcher(name, result):
        def call(*args, **kwargs):
            session = next(
                (a for a in args if hasattr(a, "in_transaction")),
                kwargs.get("session"),
            )
            held[name] = session.in_transaction() if session is not None else None
            return result
        return call

    snapshot = {"recentDeals": [{"proposedActions": [{"title": "Send the deck"}]}]}
    module = "sales_support_agent.jobs.sales_operator_review"
    with mock.patch(
        f"{module}.sync_hubspot_sales",
        watcher("hubspot", SimpleNamespace(as_dict=lambda: {"ok": True})),
    ), mock.patch(
        f"{module}.run_writeback",
        watcher("writeback", {"summary": {"candidateDeals": 1}}),
    ), mock.patch(
        f"{module}.get_operator_snapshot", watcher("snapshot", snapshot)
    ), mock.patch(
        f"{module}.send_public_tool_failure_alerts",
        watcher("alerts", {"sent": False}),
    ):
        return SalesOperatorReviewJob(SimpleNamespace(), session_factory).run(
            dry_run=False, limit=5, trigger="test"
        )


def test_no_connection_is_held_across_the_public_tool_alert_step(session_factory) -> None:
    """This is the exact step the production traceback died in."""
    held: dict = {}

    _run(session_factory, held)

    assert held["alerts"] is False, (
        "the session still held a connection through HubSpot, the writeback and "
        "the snapshot, which is what the database had already closed"
    )


def test_no_connection_is_held_across_any_external_call(session_factory) -> None:
    held: dict = {}

    _run(session_factory, held)

    holding = sorted(name for name, was_held in held.items() if was_held)
    assert holding == [], f"a connection was held across: {', '.join(holding)}"


def test_the_run_still_completes_and_is_recorded(session_factory) -> None:
    """Releasing the connection must not cost the audit trail."""
    from sales_support_agent.models.entities import AutomationRun

    result = _run(session_factory, {})

    assert result["status"] == "completed"
    assert result["next_action"] == "Send the deck"
    with session_factory() as session:
        runs = session.query(AutomationRun).all()
        assert len(runs) == 1
        assert runs[0].status == "success"


def test_the_audit_row_survives_a_run_that_dies_partway(session_factory) -> None:
    """A run that crashes in somebody else's service must still leave a record
    saying it started and failed, not nothing at all."""
    from sales_support_agent.models.entities import AutomationRun

    module = "sales_support_agent.jobs.sales_operator_review"
    with mock.patch(
        f"{module}.sync_hubspot_sales", side_effect=RuntimeError("HubSpot is down")
    ):
        result = SalesOperatorReviewJob(SimpleNamespace(), session_factory).run(
            dry_run=False, trigger="test"
        )

    assert result["status"] == "failed"
    with session_factory() as session:
        runs = session.query(AutomationRun).all()
        assert len(runs) == 1
        assert runs[0].status == "failed"
