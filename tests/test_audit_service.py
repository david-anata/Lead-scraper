import hashlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sales_support_agent.models.entities import Base
from sales_support_agent.services.audit import AuditService


def test_long_dedupe_key_is_stored_safely_and_remains_idempotent() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    long_key = "communication_comment:" + ("meeting follow-up " * 40)

    with session_factory() as session:
        audit = AuditService(session)
        action = audit.record_action(
            run_id=None,
            clickup_task_id="task-1",
            system="clickup",
            action_type="append_comment",
            dedupe_key=long_key,
        )
        session.commit()

        assert len(action.dedupe_key) == 255
        assert action.dedupe_key.endswith(
            hashlib.sha256(long_key.encode("utf-8")).hexdigest()
        )
        assert audit.has_successful_action(long_key) is True
