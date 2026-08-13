"""Compare source and target migration state without modifying either system.

Required environment variables:
    MIGRATION_SOURCE_DATABASE_URL
    MIGRATION_TARGET_DATABASE_URL

Optional artifact directories:
    MIGRATION_SOURCE_ARTIFACT_DIR
    MIGRATION_TARGET_ARTIFACT_DIR

The script prints a JSON receipt and exits non-zero when schema, row counts, or
artifact hashes differ. Database credentials are never included in the receipt.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import MetaData, Table, create_engine, inspect, select, text


SAMPLE_SIZE = 5
WATERMARK_COLUMNS = ("updated_at", "created_at", "last_sync_at", "completed_at")


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _database_snapshot(url: str) -> dict[str, Any]:
    engine = create_engine(url, pool_pre_ping=True)
    inspector = inspect(engine)
    tables: dict[str, Any] = {}
    sequences: dict[str, Any] = {}
    try:
        with engine.connect() as connection:
            for table in sorted(inspector.get_table_names()):
                quoted = inspector.dialect.identifier_preparer.quote(table)
                count = int(connection.execute(text(f"SELECT COUNT(*) FROM {quoted}")).scalar_one())
                reflected = Table(table, MetaData(), autoload_with=connection)
                primary_key = [column.name for column in reflected.primary_key.columns]
                order_columns = [reflected.c[name] for name in primary_key] or list(reflected.columns)[:1]
                sample_rows = connection.execute(
                    select(reflected).order_by(*order_columns).limit(SAMPLE_SIZE)
                ).mappings().all()
                sample_hash = hashlib.sha256(
                    json.dumps(
                        [_normalise_row(dict(row)) for row in sample_rows],
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                watermarks: dict[str, str | None] = {}
                for column_name in WATERMARK_COLUMNS:
                    if column_name in reflected.c:
                        value = connection.execute(
                            select(reflected.c[column_name]).where(
                                reflected.c[column_name].is_not(None)
                            ).order_by(reflected.c[column_name].desc()).limit(1)
                        ).scalar_one_or_none()
                        watermarks[column_name] = _normalise_value(value)
                tables[table] = {
                    "row_count": count,
                    "primary_key": primary_key,
                    "sample_size": len(sample_rows),
                    "sample_sha256": sample_hash,
                    "watermarks": watermarks,
                    "columns": [
                        {
                            "name": column["name"],
                            "type": str(column["type"]),
                            "nullable": bool(column.get("nullable", True)),
                        }
                        for column in inspector.get_columns(table)
                    ],
                    "indexes": sorted(
                        str(index.get("name") or "") for index in inspector.get_indexes(table)
                    ),
                    "foreign_keys": sorted(
                        (
                            str(key.get("referred_table") or ""),
                            tuple(key.get("constrained_columns") or ()),
                            tuple(key.get("referred_columns") or ()),
                        )
                        for key in inspector.get_foreign_keys(table)
                    ),
                }
            if engine.dialect.name == "postgresql":
                for sequence in sorted(inspector.get_sequence_names()):
                    quoted_sequence = inspector.dialect.identifier_preparer.quote(sequence)
                    row = connection.execute(
                        text(f"SELECT last_value, is_called FROM {quoted_sequence}")
                    ).first()
                    if row is not None:
                        sequences[sequence] = {
                            "last_value": int(row[0]),
                            "is_called": bool(row[1]),
                        }
    finally:
        engine.dispose()
    return {"tables": tables, "sequences": sequences}


def _normalise_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest(), "length": len(value)}
    if isinstance(value, (dict, list, tuple)):
        return json.loads(json.dumps(value, sort_keys=True, default=str))
    return str(value)


def _normalise_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _normalise_value(value) for key, value in sorted(row.items())}


def _artifact_snapshot(root_value: str) -> dict[str, Any] | None:
    if not root_value:
        return None
    root = Path(root_value).resolve()
    if not root.is_dir():
        raise RuntimeError(f"Artifact directory does not exist: {root}")
    files: dict[str, Any] = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        relative = path.relative_to(root).as_posix()
        files[relative] = {"bytes": path.stat().st_size, "sha256": digest.hexdigest()}
    return {"file_count": len(files), "files": files}


def _differences(source: dict[str, Any], target: dict[str, Any]) -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []
    source_tables = source["tables"]
    target_tables = target["tables"]
    for table in sorted(set(source_tables) | set(target_tables)):
        if table not in source_tables:
            differences.append({"kind": "extra_target_table", "table": table})
        elif table not in target_tables:
            differences.append({"kind": "missing_target_table", "table": table})
        elif source_tables[table] != target_tables[table]:
            differences.append(
                {
                    "kind": "table_mismatch",
                    "table": table,
                    "source": source_tables[table],
                    "target": target_tables[table],
                }
            )
    if source.get("sequences", {}) != target.get("sequences", {}):
        differences.append(
            {
                "kind": "sequence_mismatch",
                "source": source.get("sequences", {}),
                "target": target.get("sequences", {}),
            }
        )
    return differences


def main() -> int:
    source = _database_snapshot(_required("MIGRATION_SOURCE_DATABASE_URL"))
    target = _database_snapshot(_required("MIGRATION_TARGET_DATABASE_URL"))
    differences = _differences(source, target)
    source_artifacts = _artifact_snapshot(os.getenv("MIGRATION_SOURCE_ARTIFACT_DIR", "").strip())
    target_artifacts = _artifact_snapshot(os.getenv("MIGRATION_TARGET_ARTIFACT_DIR", "").strip())
    artifact_match = source_artifacts == target_artifacts
    if source_artifacts is not None or target_artifacts is not None:
        if not artifact_match:
            differences.append(
                {
                    "kind": "artifact_mismatch",
                    "source": source_artifacts,
                    "target": target_artifacts,
                }
            )
    receipt = {
        "ok": not differences,
        "source_table_count": len(source["tables"]),
        "target_table_count": len(target["tables"]),
        "source_sequence_count": len(source.get("sequences", {})),
        "target_sequence_count": len(target.get("sequences", {})),
        "artifact_comparison_requested": source_artifacts is not None or target_artifacts is not None,
        "artifact_match": artifact_match if source_artifacts is not None or target_artifacts is not None else None,
        "differences": differences,
    }
    print(json.dumps(receipt, indent=2, default=str))
    return 0 if receipt["ok"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # operator-readable failure without credential echo
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2), file=sys.stderr)
        raise SystemExit(2)
