from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_every_fastapi_background_task_uses_the_durable_queue() -> None:
    violations: list[str] = []
    for path in (ROOT / "sales_support_agent").rglob("*.py"):
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "background_tasks.add_task(" not in line:
                continue
            if "background_tasks.add_task(execute_durable_task," not in line:
                violations.append(f"{path.relative_to(ROOT)}:{line_number}: {line.strip()}")

    assert not violations, "FastAPI BackgroundTasks must persist intent first:\n" + "\n".join(violations)

