"""Audit logging service."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import AutomationAction, AutomationRun, IntegrationLog


class AuditService:
    def __init__(self, session: Session):
        self.session = session

    def start_run(self, run_type: str, *, trigger: str, metadata: dict[str, Any] | None = None) -> AutomationRun:
        run = AutomationRun(run_type=run_type, trigger=trigger, metadata_json=metadata or {})
        self.session.add(run)
        self.session.flush()
        return run

    def finish_run(self, run: AutomationRun, *, status: str, summary: dict[str, Any] | None = None) -> None:
        run.status = status
        run.completed_at = datetime.utcnow()
        run.summary_json = summary or {}
        self.session.add(run)

    def record_action(
        self,
        *,
        run_id: int | None,
        clickup_task_id: str,
        system: str,
        action_type: str,
        dedupe_key: str = "",
        success: bool = True,
        error_message: str = "",
        before: dict[str, Any] | None = None,
        after: dict[str, Any] | None = None,
    ) -> AutomationAction:
        action = AutomationAction(
            run_id=run_id,
            clickup_task_id=clickup_task_id,
            system=system,
            action_type=action_type,
            dedupe_key=dedupe_key,
            success=success,
            error_message=error_message,
            before_json=before or {},
            after_json=after or {},
        )
        self.session.add(action)
        self.session.flush()
        return action

    def has_successful_action(self, dedupe_key: str) -> bool:
        if not dedupe_key:
            return False
        query = select(AutomationAction.id).where(
            AutomationAction.dedupe_key == dedupe_key,
            AutomationAction.success.is_(True),
        )
        return self.session.execute(query).first() is not None

    def record_integration_log(
        self,
        *,
        run_id: int | None,
        provider: str,
        operation: str,
        status_code: int,
        success: bool,
        request_json: dict[str, Any] | None = None,
        response_json: dict[str, Any] | None = None,
    ) -> None:
        log = IntegrationLog(
            run_id=run_id,
            provider=provider,
            operation=operation,
            request_json=request_json or {},
            response_json=response_json or {},
            status_code=status_code,
            success=success,
        )
        self.session.add(log)

