from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, text

from scripts import vercel_migration_audit as audit


def _database(path: Path, rows: int) -> str:
    url = "sqlite:///" + str(path)
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE examples (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"))
        for index in range(rows):
            connection.execute(
                text("INSERT INTO examples (id, name) VALUES (:id, :name)"),
                {"id": index + 1, "name": f"Example {index + 1}"},
            )
    engine.dispose()
    return url


def test_database_snapshot_matches_equivalent_databases(tmp_path: Path) -> None:
    source = audit._database_snapshot(_database(tmp_path / "source.db", 2))
    target = audit._database_snapshot(_database(tmp_path / "target.db", 2))

    assert audit._differences(source, target) == []


def test_database_snapshot_reports_row_count_drift(tmp_path: Path) -> None:
    source = audit._database_snapshot(_database(tmp_path / "source.db", 2))
    target = audit._database_snapshot(_database(tmp_path / "target.db", 1))

    differences = audit._differences(source, target)
    assert differences[0]["kind"] == "table_mismatch"
    assert differences[0]["source"]["row_count"] == 2
    assert differences[0]["target"]["row_count"] == 1


def test_artifact_snapshot_uses_relative_paths_and_hashes(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    nested = root / "reports"
    nested.mkdir(parents=True)
    (nested / "result.json").write_text('{"ok": true}', encoding="utf-8")

    snapshot = audit._artifact_snapshot(str(root))

    assert snapshot is not None
    assert snapshot["file_count"] == 1
    assert snapshot["files"]["reports/result.json"]["bytes"] == 12
    assert len(snapshot["files"]["reports/result.json"]["sha256"]) == 64
