from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from types import SimpleNamespace

from sales_support_agent.services.fulfillment_report_storage import (
    ensure_fulfillment_report_storage_schema,
    restore_fulfillment_reports,
    snapshot_fulfillment_reports,
    FulfillmentReportStorageMiddleware,
)


def test_fulfillment_reports_round_trip_through_database(tmp_path: Path) -> None:
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    ensure_fulfillment_report_storage_schema(engine)
    source = tmp_path / "source"
    source.mkdir()
    (source / "support-review-1.json").write_text('{"status":"ready"}')
    (source / "support-review-1.html").write_text("<h1>Ready</h1>")

    saved = snapshot_fulfillment_reports(engine, source)
    restored_root = tmp_path / "restored"
    restored = restore_fulfillment_reports(engine, restored_root)

    assert saved == {"files": 2, "bytes": 32, "skipped": 0}
    assert restored == saved
    assert (restored_root / "support-review-1.json").read_text() == '{"status":"ready"}'


def test_fulfillment_restore_rejects_unsafe_paths(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    ensure_fulfillment_report_storage_schema(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO fulfillment_report_files VALUES (?, ?, ?, ?, ?)",
            ("../escape.json", b"{}", "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a", 2, "now"),
        )
    result = restore_fulfillment_reports(engine, tmp_path / "reports")
    assert result["skipped"] == 1
    assert not (tmp_path / "escape.json").exists()


def test_fulfillment_cache_hydrates_lazily(tmp_path: Path) -> None:
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    ensure_fulfillment_report_storage_schema(engine)
    source = tmp_path / "source"
    source.mkdir()
    (source / "latest.json").write_text('{"status":"ready"}')
    snapshot_fulfillment_reports(engine, source)

    target = tmp_path / "target"
    app = FastAPI()
    app.state.settings = SimpleNamespace(fulfillment_cs_reports_dir=target)
    app.state.session_factory = SimpleNamespace(kw={"bind": engine})
    app.add_middleware(FulfillmentReportStorageMiddleware)

    @app.get("/health")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/admin/fulfillment/cs/reports/")
    def reports(request: Request) -> dict[str, bool]:
        return {"hydrated": (target / "latest.json").exists()}

    client = TestClient(app)
    assert client.get("/health").json() == {"ok": True}
    assert not target.exists()
    assert client.get("/admin/fulfillment/cs/reports/").json() == {"hydrated": True}
