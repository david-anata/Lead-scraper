"""Tests for admin_auth get_session_user — covers legacy and Google SSO tokens."""
from __future__ import annotations

import base64
import hashlib
import hmac
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sales_support_agent.services.admin_auth import (
    admin_login_enabled,
    create_admin_session_token,
    create_signed_state_token,
    create_user_session_token,
    get_session_user,
    read_signed_state_token,
    validate_admin_session_token,
    verify_admin_password,
)


def _make_settings(
    *,
    username: str = "admin",
    password: str = "secret",
    secret: str = "signing-secret",
    ttl_hours: int = 24,
) -> SimpleNamespace:
    return SimpleNamespace(
        admin_username=username,
        admin_password=password,
        admin_session_secret=secret,
        admin_session_ttl_hours=ttl_hours,
    )


_NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


class TestGetSessionUserLegacy(unittest.TestCase):
    """Tests for the 3-part legacy (password-based) session token."""

    def test_valid_legacy_token_returns_admin_user(self) -> None:
        settings = _make_settings()
        token = create_admin_session_token(settings, now=_NOW)
        user = get_session_user(settings, token, now=_NOW + timedelta(hours=1))
        self.assertIsNotNone(user)
        assert user is not None
        self.assertEqual(user["email"], "admin")
        self.assertEqual(user["name"], "admin")
        self.assertEqual(user["role"], "admin")

    def test_expired_legacy_token_returns_none(self) -> None:
        settings = _make_settings()
        token = create_admin_session_token(settings, now=_NOW)
        user = get_session_user(settings, token, now=_NOW + timedelta(hours=25))
        self.assertIsNone(user)

    def test_tampered_signature_returns_none(self) -> None:
        settings = _make_settings()
        token = create_admin_session_token(settings, now=_NOW)
        # Decode, tamper, re-encode
        decoded = base64.urlsafe_b64decode(token.encode()).decode()
        parts = decoded.split("|")
        parts[-1] = "a" * len(parts[-1])  # replace signature with garbage
        tampered = base64.urlsafe_b64encode("|".join(parts).encode()).decode()
        self.assertIsNone(get_session_user(settings, tampered, now=_NOW))

    def test_wrong_username_returns_none(self) -> None:
        settings = _make_settings()
        token = create_admin_session_token(settings, now=_NOW)
        other_settings = _make_settings(username="other")
        self.assertIsNone(get_session_user(other_settings, token, now=_NOW))

    def test_malformed_token_two_parts_returns_none(self) -> None:
        settings = _make_settings()
        bad = base64.urlsafe_b64encode(b"user|12345").decode()
        self.assertIsNone(get_session_user(settings, bad, now=_NOW))

    def test_empty_token_returns_none(self) -> None:
        settings = _make_settings()
        self.assertIsNone(get_session_user(settings, "", now=_NOW))


class TestGetSessionUserGoogleSSO(unittest.TestCase):
    """Tests for the 5-part Google SSO session token."""

    def test_valid_sso_token_returns_correct_user(self) -> None:
        settings = _make_settings()
        token = create_user_session_token(
            settings,
            email="alice@example.com",
            name="Alice Smith",
            role="finance",
            now=_NOW,
        )
        user = get_session_user(settings, token, now=_NOW + timedelta(hours=1))
        self.assertIsNotNone(user)
        assert user is not None
        self.assertEqual(user["email"], "alice@example.com")
        self.assertEqual(user["name"], "Alice Smith")
        self.assertEqual(user["role"], "finance")

    def test_expired_sso_token_returns_none(self) -> None:
        settings = _make_settings()
        token = create_user_session_token(
            settings,
            email="alice@example.com",
            name="Alice Smith",
            role="finance",
            now=_NOW,
        )
        user = get_session_user(settings, token, now=_NOW + timedelta(hours=25))
        self.assertIsNone(user)

    def test_tampered_sso_token_returns_none(self) -> None:
        settings = _make_settings()
        token = create_user_session_token(
            settings,
            email="alice@example.com",
            name="Alice Smith",
            role="admin",
            now=_NOW,
        )
        decoded = base64.urlsafe_b64decode(token.encode()).decode()
        parts = decoded.split("|")
        # Tamper the role field (index 2)
        parts[2] = "superadmin"
        tampered = base64.urlsafe_b64encode("|".join(parts).encode()).decode()
        self.assertIsNone(get_session_user(settings, tampered, now=_NOW))


class TestAdminAuthHelpers(unittest.TestCase):
    def test_admin_login_enabled_requires_password_and_secret(self) -> None:
        settings = SimpleNamespace(admin_password="secret", admin_session_secret="signing-secret")
        self.assertTrue(admin_login_enabled(settings))
        self.assertFalse(admin_login_enabled(SimpleNamespace(admin_password="", admin_session_secret="signing-secret")))

    def test_verify_admin_password_uses_exact_match(self) -> None:
        settings = SimpleNamespace(admin_password="super-secret")
        self.assertTrue(verify_admin_password(settings, "super-secret"))
        self.assertFalse(verify_admin_password(settings, "wrong"))

    def test_session_token_round_trip(self) -> None:
        settings = _make_settings()
        token = create_admin_session_token(settings, now=_NOW)
        self.assertTrue(validate_admin_session_token(settings, token, now=_NOW + timedelta(hours=1)))
        self.assertFalse(validate_admin_session_token(settings, token, now=_NOW + timedelta(hours=25)))

    def test_signed_state_token_round_trip(self) -> None:
        token = create_signed_state_token("signing-secret", {"state": "abc", "code_verifier": "xyz"})
        self.assertEqual(
            read_signed_state_token("signing-secret", token),
            {"state": "abc", "code_verifier": "xyz"},
        )
        self.assertIsNone(read_signed_state_token("wrong-secret", token))


if __name__ == "__main__":
    unittest.main()
