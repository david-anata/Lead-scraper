"""Public contact intake: key gate, file validation, and #sales delivery."""

from __future__ import annotations

import base64
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/contact_public_test.db")
os.environ.setdefault("MARKETING_SITE_INTAKE_KEY", "test-intake-key")

from fastapi.testclient import TestClient

from sales_support_agent.api import contact_public_router as C
from sales_support_agent.main import app

PNG = b"\x89PNG\r\n\x1a\n" + b"0" * 100
PDF = b"%PDF-1.4\n" + b"0" * 100


def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


class ContactPublicTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)
        self.key = str(getattr(app.state.settings, "marketing_site_intake_key", "") or "")
        if not self.key:
            self.skipTest("intake key not configured in settings")
        self.headers = {"X-Internal-Api-Key": self.key}
        p = mock.patch.object(C, "durable_rate_limit_response", return_value=None)
        p.start()
        self.addCleanup(p.stop)

    def _body(self, **extra):
        body = {"name": "Test", "email": "t@example.com", "note": "Hello"}
        body.update(extra)
        return body

    def test_requires_key(self) -> None:
        r = self.client.post("/api/public/contact", json=self._body())
        self.assertEqual(r.status_code, 401)

    def test_rejects_bad_file_type(self) -> None:
        r = self.client.post("/api/public/contact", headers=self.headers,
                             json=self._body(files=[{"name": "x.exe", "data": _b64(b"MZ" + b"0" * 50)}]))
        self.assertEqual(r.status_code, 400)

    def test_rejects_too_many_and_too_large(self) -> None:
        many = [{"name": f"{i}.png", "data": _b64(PNG)} for i in range(5)]
        self.assertEqual(self.client.post("/api/public/contact", headers=self.headers,
                                          json=self._body(files=many)).status_code, 400)
        files, err = C.decode_files([{"name": "a.pdf", "data": _b64(b"%PDF-" + b"0" * 3_000_001)}])
        self.assertTrue(err)

    def test_honeypot_is_silent(self) -> None:
        with mock.patch.object(C.SlackClient, "post_message") as post:
            r = self.client.post("/api/public/contact", headers=self.headers, json=self._body(company_website="x"))
        self.assertEqual(r.status_code, 202)
        post.assert_not_called()

    def test_posts_files_to_sales(self) -> None:
        with mock.patch.object(C.SlackClient, "is_configured", return_value=True), \
             mock.patch.object(C.SlackClient, "upload_files", return_value={"ok": True}) as up:
            r = self.client.post("/api/public/contact", headers=self.headers, json=self._body(
                files=[{"name": "a.png", "data": _b64(PNG)}, {"name": "b", "data": _b64(PDF)}]))
        self.assertEqual(r.status_code, 200, r.text)
        kwargs = up.call_args.kwargs
        self.assertEqual(kwargs["channel"], C.SALES_SLACK_CHANNEL)
        self.assertEqual([f[0] for f in kwargs["files"]], ["a.png", "b.pdf"])

    def test_posts_note_without_files(self) -> None:
        with mock.patch.object(C.SlackClient, "is_configured", return_value=True), \
             mock.patch.object(C.SlackClient, "post_message", return_value={"ok": True}) as post:
            r = self.client.post("/api/public/contact", headers=self.headers, json=self._body())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(post.call_args.kwargs["channel"], C.SALES_SLACK_CHANNEL)


if __name__ == "__main__":
    unittest.main()
