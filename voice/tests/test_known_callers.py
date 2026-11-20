"""Directory routing: category defaults, overrides, and the unknown insert."""

from voice.known_callers import (
    apply_known_caller,
    effective_ring_policy,
    known_greeting,
    looks_like_tenant_urgent,
)
from voice.lines import LineContext
from voice.postgres_log import PostgresLog
from voice.screening import decide
from voice.session import CallSession
from voice.tests.test_screening import _screen


def _call(**overrides: object) -> CallSession:
    session = CallSession(
        call_sid="CA1",
        caller_number="+18015550100",
        dialed_number="+13853363562",
        line=LineContext(id="anata", label="Anata"),
    )
    for key, value in overrides.items():
        setattr(session, key, value)
    return session


def test_ring_policy_override_wins_over_the_category_default() -> None:
    assert effective_ring_policy("tenant", None) == "urgent_only"
    assert effective_ring_policy("client", "") == "always"
    assert effective_ring_policy("vendor", "always") == "always"
    assert effective_ring_policy("vip", "never") == "never"
    assert effective_ring_policy("unknown", None) == ""


def test_known_greetings_use_the_directory_name() -> None:
    tenant = _call()
    apply_known_caller(
        tenant,
        {"id": "k1", "name": "Jamie Lee", "company": "North", "category": "tenant", "ring_policy": ""},
    )
    assert known_greeting(tenant) == "Hi Jamie. You've reached Anata. What's going on?"
    assert tenant.ring_policy == "urgent_only"
    client = _call()
    apply_known_caller(
        client,
        {"id": "k2", "name": "Jamie Lee", "company": "North", "category": "client", "ring_policy": ""},
    )
    assert known_greeting(client) == "Hi Jamie Lee. Let me see if I can grab David, one moment."
    assert client.ring_policy == "always"
    legal = _call()
    apply_known_caller(legal, {"id": "k3", "name": "Pat", "company": "", "category": "landlord_legal", "ring_policy": ""})
    assert known_greeting(legal).startswith("Hello, Pat.")
    assert "rent" not in known_greeting(legal).casefold()
    vendor = _call()
    apply_known_caller(vendor, {"id": "k4", "name": "Sam", "company": "Ink", "category": "vendor", "ring_policy": ""})
    assert vendor.ring_policy == "never"
    assert "message" in known_greeting(vendor)


def test_tenant_urgent_only_rings_for_a_lockout_and_not_for_mail() -> None:
    assert looks_like_tenant_urgent("We are locked out of the suite")
    urgent = decide(
        _screen(
            category="tenant",
            reason="We are locked out",
            ring_policy="urgent_only",
            known_category="tenant",
            wants_live_person=True,
        )
    )
    routine = decide(
        _screen(
            category="tenant",
            reason="Where is the mail room",
            ring_policy="urgent_only",
            known_category="tenant",
            wants_live_person=True,
            is_current_tenant=True,
        )
    )
    assert urgent.allow_transfer is True
    assert urgent.asked_for_david is True
    assert routine.allow_transfer is False
    assert routine.action == "needs_callback"


def test_client_rings_with_no_screening_and_a_vendor_does_not() -> None:
    client = decide(
        _screen(
            category="lead",
            reason="We do SEO and can rank you on Google",
            ring_policy="always",
            known_category="client",
        )
    )
    vendor = decide(
        _screen(
            category="vendor",
            reason="Fire alarm in the lobby",
            ring_policy="never",
            known_category="vendor",
            is_building_emergency=True,
        )
    )
    assert client.allow_transfer is True
    assert vendor.allow_transfer is False


def test_a_new_number_is_inserted_as_unknown_with_parameters() -> None:
    class _Cursor:
        def __init__(self, conn: "_Conn") -> None:
            self.conn = conn
            self._row = None

        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            self.conn.statements.append((sql, params))
            assert "+18015550111" not in sql
            if sql.startswith("SELECT") and not self.conn.inserted:
                self._row = None
            elif sql.startswith("INSERT"):
                self.conn.inserted = True
                self._row = ("new-id",)
            else:
                self._row = ("new-id", "Ada", "Co", "client", "always")

        def fetchone(self):
            return self._row

        def __enter__(self) -> "_Cursor":
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    class _Conn:
        def __init__(self) -> None:
            self.statements: list[tuple[str, object]] = []
            self.inserted = False
            self.committed = False

        def cursor(self) -> _Cursor:
            return _Cursor(self)

        def commit(self) -> None:
            self.committed = True

        def __enter__(self) -> "_Conn":
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    conn = _Conn()
    writer = PostgresLog("postgresql://voice_logger@pooler/db", connect=lambda dsn: conn)
    created = writer.resolve_caller("8015550111")
    assert created["id"] == "new-id"
    assert created["category"] == "unknown"
    assert conn.committed
    insert = next(sql for sql, _params in conn.statements if sql.startswith("INSERT"))
    assert "category" in insert and "'unknown'" in insert
    assert "inbound_call" in insert
    assert conn.statements[1][1] == ("", "+18015550111")
