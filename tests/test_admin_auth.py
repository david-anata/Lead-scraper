from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sales_support_agent.services.admin_auth import (
    admin_login_enabled,
    create_admin_session_token,
    validate_admin_session_token,
    verify_admin_password,
)


class AdminAuthTests(unittest.TestCase):
    def test_admin_login_enabled_requires_password_and_secret(self) -> None:
        settings = SimpleNamespace(admin_password="secret", admin_session_secret="signing-secret")
        self.assertTrue(admin_login_enabled(settings))
        self.assertFalse(admin_login_enabled(SimpleNamespace(admin_password="", admin_session_secret="signing-secret")))

    def test_verify_admin_password_uses_exact_match(self) -> None:
        settings = SimpleNamespace(admin_password="super-secret")
        self.assertTrue(verify_admin_password(settings, "super-secret"))
        self.assertFalse(verify_admin_password(settings, "wrong"))

    def test_session_token_round_trip(self) -> None:
        settings = SimpleNamespace(
            admin_username="admin",
            admin_session_secret="signing-secret",
            admin_session_ttl_hours=24,
        )
        issued_at = datetime(2026, 3, 14, 12, 0, tzinfo=timezone.utc)
        token = create_admin_session_token(settings, now=issued_at)

        self.assertTrue(validate_admin_session_token(settings, token, now=issued_at + timedelta(hours=1)))
        self.assertFalse(validate_admin_session_token(settings, token, now=issued_at + timedelta(hours=25)))


if __name__ == "__main__":
    unittest.main()
